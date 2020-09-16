package com.qimok.migrate.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qimok.migrate.jdbc.JdbcConnection;
import com.qimok.migrate.ready.SubTableParameters;
import com.qimok.migrate.ready.migrate.MessageAllSubTableMigrateReady;
import com.qimok.migrate.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import static com.qimok.migrate.model.ModelConstants.MESSAGE_ID_MAPPING_SINGLE_TABLE_CAPACITY;

/**
 * @author qimok
 * @description 数据迁移执行器(一个总表的数据同时迁移到各个分表及映射表中)
 * @since 2020-06-01
 */
@Slf4j
@Component
public class DataMigrateAllSubTableExecutingService {

    @Autowired
    private JdbcConnection conn;

    @Autowired
    private MessageAllSubTableMigrateReady ready;

    public void executeMigrate(String task, Long offset, Long currId, Long idLe, Integer everyCommitCount,
                               List<Long> groupIdIn, Optional<Long> createdGte) {
        long startTime = System.currentTimeMillis();
        Connection sourceConn = null;
        Connection targetConn = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            sourceConn = conn.getSourceConn();
            targetConn = conn.getTargetConn();
            targetConn.setAutoCommit(false);
            String querySql = ready.getQuerySql(task, currId, idLe, groupIdIn, createdGte);
            stmt = sourceConn.createStatement();
            long readStartTime = System.currentTimeMillis();
            stmt.execute(querySql);
            long readEndTime = System.currentTimeMillis();
            log.info(String.format(task + " ==> 读花费时长：%s 毫秒", readEndTime - readStartTime));
            res = stmt.getResultSet();
            long executeStartTime = System.currentTimeMillis();
            // 获取各分片数据集
            Map<String, Map<Integer, List<Object[]>>> subTableDataMap = parseSubTableData(res);
            batchInsertSubTables(targetConn, subTableDataMap, ready.getMessageSubTableParameters(), everyCommitCount);
            batchInsertSubTables(targetConn, subTableDataMap, ready.getMessageIdSubTableParameters(), everyCommitCount);
            batchInsertSubTables(targetConn, subTableDataMap, ready.getGuidSubTableParameters(), everyCommitCount);
            // 批量向所有分表中插入数据
            long executeEndTime = System.currentTimeMillis();
            log.info(String.format(task + " ==> 每组执行花费时长(每组执行 %s 条数据)：%s 毫秒", everyCommitCount,
                    executeEndTime - executeStartTime));
        } catch (Exception e) {
            log.error(String.format("%s ==> 错误，【offset: %s】", task, offset), e);
        } finally {
            conn.closeConn(sourceConn, targetConn, stmt, res, null);
            long endTime = System.currentTimeMillis(); // 记录程序结束时间
            log.info(String.format(task + " ==> 【offset: %s】，消耗时长：%s 秒",
                    offset, String.format("%.4f", (endTime - startTime) / 1000d)));
        }
    }

    /**
     * 获取各分片数据集
     */
    private Map<String, Map<Integer, List<Object[]>>> parseSubTableData(ResultSet res) throws SQLException {
        Map<String, Map<Integer, List<Object[]>>> map = Maps.newHashMap();
        // 各分表数据
        Map<Integer, List<Object[]>> messageSubTableDataMap = Maps.newHashMap();
        Map<Integer, List<Object[]>> messageIdSubTableDataMap = Maps.newHashMap();
        Map<Integer, List<Object[]>> guidSubTableDataMap = Maps.newHashMap();
        while (res.next()) {
            messageSubTableDataMap = parseData(res, messageSubTableDataMap,
                    Integer.parseInt(res.getObject("t4").toString())
                            % ready.getMessageSubTableParameters().getSubTableNum(),
                    ready.getMessageSubTableParameters().getFieldNum());

            messageIdSubTableDataMap = parseData(res, messageIdSubTableDataMap,
                    getRangeTableSuffix(Integer.parseInt(res.getObject("t1").toString())),
                    ready.getMessageIdSubTableParameters().getFieldNum());

            guidSubTableDataMap = parseData(res, guidSubTableDataMap,
                    StringUtil.hexStrMod(String.valueOf(res.getObject("t3")).split("-")[0],
                            ready.getGuidSubTableParameters().getSubTableNum()),
                    ready.getGuidSubTableParameters().getFieldNum());
        }
        map.put(ready.getMessageSubTableParameters().getShardingColumn(), messageSubTableDataMap);
        map.put(ready.getMessageIdSubTableParameters().getShardingColumn(), messageIdSubTableDataMap);
        map.put(ready.getGuidSubTableParameters().getShardingColumn(), guidSubTableDataMap);
        return map;
    }

    private Map<Integer, List<Object[]>> parseData(ResultSet res, Map<Integer, List<Object[]>> subTableDataMap,
                                                   Integer subTableSuffix, Integer fieldNum) throws SQLException {
        Object[] data = new Object[fieldNum + 1];
        if (fieldNum == 18) {
            for (int i = 1; i <= fieldNum; i++) {
                data[i] = res.getObject("t" + i);
            }
        } else if (fieldNum == 3) {
            data[1] = res.getObject("t1");
            data[2] = res.getObject("t4");
            data[3] = res.getObject("t17");
        } else if (fieldNum == 2) {
            data[1] = res.getObject("t3");
            data[2] = res.getObject("t4");
        } else {
            throw new UnsupportedOperationException();
        }
        if (Objects.nonNull(subTableSuffix)) {
            if (subTableDataMap.containsKey(subTableSuffix)) {
                subTableDataMap.get(subTableSuffix).add(data);
            } else {
                List<Object[]> dataList = Lists.newArrayList();
                dataList.add(data);
                subTableDataMap.put(subTableSuffix, dataList);
            }
        }
        return subTableDataMap;
    }

    /**
     * 批量向所有分表中插入数据
     */
    private void batchInsertSubTables(Connection targetConn, Map<String, Map<Integer, List<Object[]>>> subTableDataMap,
                                      SubTableParameters parameters, Integer everyCommitCount) throws SQLException {
        Map<Integer, List<Object[]>> dataMap = subTableDataMap.get(parameters.getShardingColumn());
        if (!dataMap.isEmpty()) {
            for (Integer key : dataMap.keySet()) {
                // 预编译
                try(PreparedStatement pstmt = targetConn
                        .prepareStatement(parameters.getExecSql().replaceFirst("\\*", String.valueOf(key)))) {
                    List<Object[]> dataList = dataMap.get(key);
                    int num = 0;
                    for (int i = 0; i < dataList.size(); i++) {
                        num++;
                        Object[] objects = dataList.get(i);
                        for (int j = 1; j < dataList.get(i).length; j++) {
                            pstmt.setObject(j, objects[j]);
                        }
                        pstmt.addBatch();
                        if (num > everyCommitCount) {
                            pstmt.executeBatch();
                            targetConn.commit();
                            pstmt.clearBatch();
                            num = 0;
                        }
                    }
                    pstmt.executeBatch();
                    targetConn.commit();
                }
            }
        }
    }

    private Integer getRangeTableSuffix(Integer shardValue) {
        Long subTableSuffix = shardValue / MESSAGE_ID_MAPPING_SINGLE_TABLE_CAPACITY;
        if (subTableSuffix <= 12) {
            return Integer.valueOf(subTableSuffix.toString());
        }
        throw new UnsupportedOperationException();
    }

}

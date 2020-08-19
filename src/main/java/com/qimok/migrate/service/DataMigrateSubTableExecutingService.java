package com.qimok.migrate.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qimok.migrate.jdbc.JdbcConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.sql.*;
import java.util.*;

/**
 * @author qimok
 * @description 数据迁移执行器
 * @since 2020-06-1 10:07
 */
@Slf4j
@Service
public class DataMigrateSubTableExecutingService {

    @Autowired
    private JdbcConnection conn;

    @Autowired
    private DataMigrateTransferringService transferringService;

    public void executeMigrate(String task, Long offset, Long currId, Long idLe, Integer everyCommitCount,
                               List<Long> groupIdIn, Optional<Long> createdGte) {
        long startTime = System.currentTimeMillis();
        Connection sourceConn = null;
        Connection targetConn = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            IDataMigrateReady ready = transferringService.toReady(task);
            Integer sourceFlag = ready.getSourceFlag(task);
            sourceConn = sourceFlag == 0 ? conn.getTargetConn() : conn.getSourceConn();
            targetConn = conn.getTargetConn();
            targetConn.setAutoCommit(false);
            String querySql = ready.getQuerySql(task, currId, idLe, groupIdIn, createdGte);
            String execSql = ready.getExecSql();
            if (currId == 0 || (currId > everyCommitCount && currId % everyCommitCount == 0)) {
                log.info(task + " ==> " + currId + " QuerySQL >> \n" + querySql);
                log.info(task + " ==> " + currId + " ExecSQL >> \n" + execSql);
            }
            stmt = sourceConn.createStatement();
            long readStartTime = System.currentTimeMillis();
            stmt.execute(querySql); // 执行sql
            long readEndTime = System.currentTimeMillis();
            log.info(String.format(task + " ==> 读花费时长：%s 毫秒", readEndTime - readStartTime));
            res = stmt.getResultSet();
            long executeStartTime = System.currentTimeMillis();

            // 获取各分片数据集
            Map<Integer, List<Object[]>> subTableDataMap = getSubTableData(res,
                    ready.getSubTableNum(), ready.getFieldNum());
            // 批量向所有分表中插入数据
            batchInsertSubTables(targetConn, execSql, ready.getSubTableNum(), subTableDataMap, everyCommitCount);

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
    private Map<Integer, List<Object[]>> getSubTableData(ResultSet res, Integer shardNum, Integer fieldNum)
            throws SQLException {
        // 各分表数据
        Map<Integer, List<Object[]>> subTableDataMap = Maps.newHashMap();
        while (res.next()) {
            Integer shardValue = Integer.parseInt(res.getObject("shardingColumn").toString());
            Integer subTableSuffix = shardValue % shardNum;
            Object[] data = new Object[fieldNum + 1];
            for (int i = 1; i <= fieldNum; i++) {
                data[i] = res.getObject("t" + i);
            }
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
    private void batchInsertSubTables(Connection targetConn, String execSql, Integer shardNum,
                                      Map<Integer, List<Object[]>> subTableDataMap,
                                      Integer everyCommitCount) throws SQLException {
        if (!subTableDataMap.isEmpty()) {
            for (Integer key : subTableDataMap.keySet()) {
                if (subTableDataMap.containsKey(key)) {
                    // 预编译
                    try(PreparedStatement pstmt = targetConn
                            .prepareStatement(execSql.replaceFirst("\\*", String.valueOf(key)))) {
                        List<Object[]> dataList = subTableDataMap.get(key);
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
    }

}

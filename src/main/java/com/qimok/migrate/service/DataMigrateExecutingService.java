package com.qimok.migrate.service;

import com.qimok.migrate.jdbc.JdbcConnection;
import com.qimok.migrate.ready.IDataMigrateReady;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

/**
 * @author qimok
 * @description 数据迁移执行器(单表数据迁移 or 数据清理)
 * @since 2020-06-1 10:07
 */
@Slf4j
@Service
public class DataMigrateExecutingService {

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
        PreparedStatement pstmt = null;
        Long currMaxId = 0L;
        Long count = 0L; // 迁移数量
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
            pstmt = targetConn.prepareStatement(execSql); // 预编译
            int num = 0;
            while (res.next()) {
                num++;
                count++;
                for (int i = 1; i <= ready.getFieldNum(); i++) {
                    pstmt.setObject(i, res.getObject("t" + i));
                }
                pstmt.addBatch();
                // 每 everyCommitCount 条数据提交一次事务
                if (num > everyCommitCount) {
                    long executeStartTime = System.currentTimeMillis();
                    pstmt.executeBatch();
                    targetConn.commit();
                    pstmt.clearBatch();
                    long executeEndTime = System.currentTimeMillis();
                    log.info(String.format(task + " ==> 每组执行花费时长(每组执行 %s 条数据)：%s 毫秒", everyCommitCount,
                            executeEndTime - executeStartTime));
                    num = 0;
                }
                currMaxId = Math.max(res.getLong("t1"), currMaxId);
            }
            pstmt.executeBatch();
            targetConn.commit();
        } catch (Exception e) {
            log.error(String.format("%s ==> 错误，【offset: %s】", task, offset), e);
        } finally {
            conn.closeConn(sourceConn, targetConn, stmt, res, pstmt);
            long endTime = System.currentTimeMillis(); // 记录程序结束时间
            log.info(String.format(task + " ==> 【offset: %s】，单次总共扫描了：%s 条数据，消耗时长：%s 秒",
                    offset, count, String.format("%.4f", (endTime - startTime) / 1000d)));
        }
    }

}

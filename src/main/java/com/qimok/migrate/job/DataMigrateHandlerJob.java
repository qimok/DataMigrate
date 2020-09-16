package com.qimok.migrate.job;

import com.google.common.collect.Lists;
import com.qimok.migrate.redis.RedisService;
import com.qimok.migrate.service.DataMigrateAllSubTableExecutingService;
import com.qimok.migrate.service.DataMigrateExecutingService;
import com.qimok.migrate.service.DataMigrateSubTableExecutingService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.qimok.migrate.model.ModelConstants.*;

/**
 * @author qimok
 * @description 数据迁移Job
 * @since 2020-06-1 10:07
 */
@Slf4j
@Component
public class DataMigrateHandlerJob {

    @Value("${data.migrate.enabled:false}")
    private Boolean enabled;

    @Value("${data.migrate.threadNum:4}")
    private Integer threadNum;

    @Value("${data.migrate.qps:900}")
    private Integer qps;

    @Value("${data.migrate.everyCommitCount:1500}")
    private Integer everyCommitCount;

    @Value("${data.migrate.is.sub.table:false}")
    private Boolean isSubTable;

    @Autowired
    private RedisService redisService;

    @Autowired
    private DataMigrateExecutingService executingService;

    @Autowired
    private DataMigrateSubTableExecutingService subTableExecutingService;

    @Autowired
    private DataMigrateAllSubTableExecutingService allSubTableExecutingService;

    /**
     * 数据迁移，不重复
     * <p>
     * 每10秒执行一次
     */
    @Scheduled(fixedRate = 1000 * 10)
    public void process() {
        String target = redisService.getValue(DATA_MIGRATE_TASK);
        if (enabled == null || !enabled && (StringUtils.isBlank(target) || target.equals("0"))) {
            return;
        }
        /**
         * task 中第一个"." 前面假如为数字，则有特殊含义，执行标识：execFlag
         * <p>
         *     0：默认(多实例、多线程生序执行)
         *     1：代表单实例、单线程执行
         *     2：代表降序执行
         *     ...
         */
        String firstStr = target.substring(0, target.indexOf("."));
        Integer execFlag = org.apache.commons.lang3.StringUtils.isNumeric(firstStr) ? Integer.valueOf(firstStr) : 0;
        // 单实例、单线程执行任务
        if ((execFlag & 1) == 1) {
            Boolean isExec = redisService.setIfAbsent(target, target, 3000L);
            if (isExec) {
                // 当前实例抢到执行权
                try {
                    shardingMigrate(target, execFlag, threadNum, qps, everyCommitCount);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    redisService.delKey(target);
                }
            }
            return;
        }
        // 多实例、多线程执行任务
        shardingMigrate(target, execFlag, threadNum, qps, everyCommitCount);
    }

    private void shardingMigrate(String target, Integer execFlag,
                                 Integer threadNum, Integer qps, Integer everyCommitCount) {
        long beginTime = System.currentTimeMillis();
        String commLog = target + " ==> 数据迁移>>开始时间（" + OffsetDateTime.now().toLocalDateTime() + "）";
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        CountDownLatch latch = new CountDownLatch(threadNum);
        try {
            for (int i = 0; i < threadNum; i++) {
                executorService.submit(() -> {
                    Long solveCount = qps / 4 * 60L;
                    Long offset;
                    if ((execFlag & 2) == 2) {
                        // 当执行标识为 2 时，表示降序执行任务
                        offset = redisService.decr(DATA_MIGRATE_OFFSET);
                    } else {
                        // 默认升序执行任务
                        offset = redisService.incr(DATA_MIGRATE_OFFSET);
                    }
                    // offset == 1L 表示刚开始迁移，当前ID需要设置为0
                    Long currId = offset == 0L ? 0L : (offset - 1) * solveCount;
                    Long idLe = offset * solveCount;
                    log.info(String.format(target + " ==> 数据迁移【offset: %s，处理的ID范围：(%s, %s]】->",
                            offset, currId, idLe) + "开始...");
                    if (isSubTable) {
                        // 【往水平分表的各个分表中迁移数据】sourceTable 与 targetTable：一对多
                        if (target.equals(MESSAGE_ALL_SUB_TABLE_MIGRATE_READY)) {
                            // 同时进行
                            allSubTableExecutingService.executeMigrate(target, offset, currId, idLe, everyCommitCount,
                                    Lists.newArrayList(), Optional.empty());
                        } else {
                            // 按分表进行
                            subTableExecutingService.executeMigrate(target, offset, currId, idLe, everyCommitCount,
                                    Lists.newArrayList(), Optional.empty());
                        }
                    } else {
                        // sourceTable 与 targetTable：一对一
                        executingService.executeMigrate(target, offset, currId, idLe, everyCommitCount,
                                Lists.newArrayList(), Optional.empty());
                    }
                    latch.countDown();
                });
            }
            latch.await();
        } catch (InterruptedException e) {
            log.error(commLog + "整体异常【offset: %s】", e);
        } finally {
            long endTime = System.currentTimeMillis();
            log.info(String.format(commLog + " ==> 线程数量：%s 个,总花费时长：%s 秒",
                    threadNum, String.format("%.4f", (endTime - beginTime) / 1000d)));
            executorService.shutdown();
        }
    }

}

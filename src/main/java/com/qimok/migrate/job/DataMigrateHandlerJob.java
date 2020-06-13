package com.qimok.migrate.job;

import com.google.common.collect.Lists;
import com.qimok.migrate.redis.RedisService;
import com.qimok.migrate.service.DataMigrateExecutingService;
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

    @Autowired
    private RedisService redisService;

    @Autowired
    private DataMigrateExecutingService executingService;

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
        Integer execFlag = StringUtils.isNumeric(firstStr) ? Integer.valueOf(firstStr) : 0;
        // 假如是需要单实例执行的任务，需要抢占锁，抢占成功，则继续；抢占失败，则结束
        if ((execFlag & 1) == 1 && !isCurrInstanceExec(target)) {
            // 当执行标识为 1 时，当前实例没有抢到执行权，则退出任务
            return;
        }
        // 当执行标识为 1 时，需要指定线程数量为 1
        threadNum = (execFlag & 1) == 1 ? 1 : threadNum;
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
                    executingService.executeMigrate(target, offset, currId, idLe, everyCommitCount,
                            Lists.newArrayList(), Optional.empty());
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

    /**
     * 当前实例是否可以执行
     */
    private Boolean isCurrInstanceExec(String target) {
        return redisService.setIfAbsent(target, target, 3000L);
    }

}

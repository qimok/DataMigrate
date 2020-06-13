package com.qimok.migrate.service;

import com.google.common.collect.Lists;
import com.qimok.migrate.form.DataRepairForm;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Optional;
import static com.qimok.migrate.model.ModelConstants.*;

/**
 * @author qimok
 * @description 精确/增量修复数据
 * @since 2020-06-1 10:07
 */
@Slf4j
@Service
public class DataRepairingService {

    @Autowired
    private DataMigrateExecutingService executingService;

    private static final Integer EVERY_COMMIT_COUNT = 100;

    public void repairMessages(DataRepairForm form) {
        List<Integer> repairFlagIn = form.getRepairFlagIn();
        List<Long> groupIdIn = form.getGroupIdIn();
        Optional<Long> createdGte = form.getCreatedGte();
        List<String> tasks = Lists.newArrayList();
        repairFlagIn.forEach(flag -> {
            if (repairFlagIn.contains(0) || repairFlagIn.contains(1)) {
                tasks.add(MESSAGE_MIGRATE);
            }
            // 这里可以有很多任务
        });
        tasks.forEach(task -> executingService.executeMigrate(task, null, 0L, null,
                EVERY_COMMIT_COUNT, groupIdIn, createdGte));
    }

}

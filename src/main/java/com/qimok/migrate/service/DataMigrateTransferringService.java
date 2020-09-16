package com.qimok.migrate.service;

import com.qimok.migrate.ready.IDataMigrateReady;
import com.qimok.migrate.ready.clean.MessageCleanReady;
import com.qimok.migrate.ready.migrate.MessageAllSubTableMigrateReady;
import com.qimok.migrate.ready.migrate.MessageMigrateReady;
import com.qimok.migrate.ready.migrate.MessageSubTableMigrateReady;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import static com.qimok.migrate.model.ModelConstants.*;

/**
 * @author qimok
 * @description 数据迁移 task 策略配置类
 * @since 2020-06-1 10:07
 */
@Service
public class DataMigrateTransferringService {

    @Autowired
    private MessageMigrateReady messageMigrateReady;

    @Autowired
    private MessageSubTableMigrateReady messageSubTableMigrateReady;

    @Autowired
    private MessageAllSubTableMigrateReady messageAllSubTableMigrateReady;

    @Autowired
    private MessageCleanReady messageCleanReady;

    /**
     * 转化对应的 Service
     */
    public IDataMigrateReady toReady(String task) throws Exception {
        switch (task) {
            // -------------- 迁移任务 --------------
            case MESSAGE_MIGRATE:
                return messageMigrateReady;
            case MESSAGE_SUB_TABLE_MIGRATE:
                return messageSubTableMigrateReady;
            case MESSAGE_ALL_SUB_TABLE_MIGRATE_READY:
                return messageAllSubTableMigrateReady;
            // -------------- 清理任务 --------------
            case MESSAGE_CLEAN:
                return messageCleanReady;
            default:
                break;
        }
        throw new Exception(">>> 没有此项任务，请检查 <<<");
    }

}

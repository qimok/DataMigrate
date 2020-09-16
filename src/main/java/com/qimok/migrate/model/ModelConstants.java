package com.qimok.migrate.model;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ModelConstants {

    /**
     * 当前要进行数据迁移的 task
     */
    public static final String DATA_MIGRATE_TASK = "data:migrate:task";

    /**
     * 公用的迁移位置
     */
    public static final String DATA_MIGRATE_OFFSET = "data:migrate:offset";

    /**
     * 消息表迁移的 task
     */
    public static final String MESSAGE_MIGRATE = "message.migrate";

    /**
     * 消息分表迁移的 task（分多次完成）
     */
    public static final String MESSAGE_SUB_TABLE_MIGRATE = "message.sub.table.migrate";

    /**
     * 消息分表迁移的 task（一次性完成）
     */
    public static final String MESSAGE_ALL_SUB_TABLE_MIGRATE_READY = "message.all.sub.table.migrate.ready";

    /**
     * 清理脏数据的 task
     */
    public static final String MESSAGE_CLEAN = "message.clean";

    /**
     * message_id_mapping 单表数量
     */
    public static final Long MESSAGE_ID_MAPPING_SINGLE_TABLE_CAPACITY = 20000000L;

}

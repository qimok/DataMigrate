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
     * 消息分表迁移的 task
     */
    public static final String MESSAGE_SUB_TABLE_MIGRATE = "message.sub.table.migrate";

    /**
     * 清理脏数据的 task
     */
    public static final String MESSAGE_CLEAN = "message.clean";

}

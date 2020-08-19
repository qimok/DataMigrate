package com.qimok.migrate.ready.migrate;

import com.qimok.migrate.service.IDataMigrateReady;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * @author qimok
 * @description 数据迁移准备(数据来源SQL组装、数据执行SQL、影响字段的数目、是否跨实例)
 * @since 2020-06-1 10:07
 */
@Component
public class MessageSubTableMigrateReady implements IDataMigrateReady {

    /**
     * 特别指出：假如 source 库与target 库不在同一个实例，且 source 数据组装依赖 target 库表的数据时，可用如下方式
     */

    @Override
    public String getQuerySql(String target, Long currId, Long idLe, List<Long> groupIdIn, Optional<Long> createdGte) {
        return "select session_root_id as shardingColumn, message_id as t1, guid as t2, \n" +
                "session_root_id as t3,  \n" +
                "session_id as t4, consult_channel as t5, sender_id as t6, \n" +
                "sender_role as t7, sender as t8, message_source as t9, message_type as t10,  \n" +
                "content as t11, status as t12, sender_visible as t13, \n" +
                "visible_roles as t14, extra as t15\n" +
                "from sharding_test.message where id >= " + currId + " and id <= " + idLe;
    }

    @Override
    public String getExecSql() {
        return "insert ignore into sharding.message_* (message_id, out_message_id, guid, session_root_id,  \n" +
                "session_id, consult_channel, sender_id, sender_role, sender, message_source, message_type,  \n" +
                "content, status, sender_visible, visible_roles, extra) Values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    }

    @Override
    public Integer getFieldNum() {
        return 15;
    }

    public Integer getSourceFlag(String target) {
        return 1;
    }

}

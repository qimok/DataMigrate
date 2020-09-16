package com.qimok.migrate.ready.migrate;

import com.qimok.migrate.ready.IDataMigrateReady;
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
                "aaa as t4, bbb as t5, ccc as t6, \n" +
                "ddd as t7, eee as t8, fff as t9, ggg as t10,  \n" +
                "content as t11, hhh as t12, iii as t13, \n" +
                "jjj as t14, kkk as t15\n" +
                "from message where id >= " + currId + " and id <= " + idLe;
    }

    @Override
    public String getExecSql() {
        return "insert ignore into message_* (message_id, out_message_id, guid, session_root_id,  \n" +
                "aaa, bbb, ccc, ddd, eee, fff, ggg,  \n" +
                "content, hhh, iii, jjj, kkk) Values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    }

    @Override
    public Integer getFieldNum() {
        return 15;
    }

    public Integer getSourceFlag(String target) {
        return 1;
    }

}

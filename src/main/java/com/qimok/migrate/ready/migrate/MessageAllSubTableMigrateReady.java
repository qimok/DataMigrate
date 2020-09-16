package com.qimok.migrate.ready.migrate;

import com.qimok.migrate.ready.SubTableParameters;
import com.qimok.migrate.ready.IDataMigrateReady;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Optional;

/**
 * @author qimok
 * @since 2020-06-1
 */
@Component
public class MessageAllSubTableMigrateReady implements IDataMigrateReady {

    @Override
    public String getQuerySql(String target, Long currId, Long idLe, List<Long> groupIdIn, Optional<Long> createdGte) {
        return "select message_id as t1, aaa as t2, \n" +
                "guid as t3, session_root_id as t4, bbb as t5, ccc as t6, \n" +
                "ddd as t7, eee as t8, fff as t9, ggg as t10, hhh as t11, \n" +
                "content as t12, iii as t13, jjj as t14, \n" +
                " kkk as t15, lll as t16, \n" +
                " created as t17, mmm as t18\n" +
                "from message where message_id >= " + currId + " and message_id <= " + idLe;
    }

    @Override
    public String getExecSql() {
        return null;
    }

    /**
     * message 分表（主表）
     */
    public SubTableParameters getMessageSubTableParameters() {
        return SubTableParameters.builder()
                .execSql("insert ignore into message_* (message_id, aaa, guid, session_root_id, bbb, \n" +
                        "ccc, ddd, eee, fff, ggg, hhh, content, iii, \n" +
                        "jjj, kkk, lll, created, mmm) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .shardingColumn("session_root_id")
                .subTableNum(32)
                .shardingAlgorithm(0)
                .fieldNum(18)
                .build();
    }

    /**
     * message_id_mapping 分表（映射表）
     */
    public SubTableParameters getMessageIdSubTableParameters() {
        return SubTableParameters.builder()
                .execSql("insert ignore into message_id_mapping_* (message_id, session_root_id, created)" +
                        " VALUES (?,?,?)")
                .shardingColumn("message_id")
                .subTableNum(12)
                .shardingAlgorithm(1)
                .fieldNum(3)
                .build();
    }

    /**
     * guid_mapping 分表（映射表）
     */
    public SubTableParameters getGuidSubTableParameters() {
        return SubTableParameters.builder()
                .execSql("insert ignore into guid_mapping_* (guid, session_root_id)" +
                        " VALUES (?,?)")
                .shardingColumn("guid")
                .subTableNum(32)
                .shardingAlgorithm(0)
                .fieldNum(2)
                .build();
    }

}

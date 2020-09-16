package com.qimok.migrate.ready.migrate;

import com.qimok.migrate.ready.IDataMigrateReady;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Optional;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

/**
 * @author qimok
 * @description 数据迁移准备(数据来源SQL组装、数据执行SQL、影响字段的数目、是否跨实例)
 * @since 2020-06-1 10:07
 */
@Component
public class MessageMigrateReady implements IDataMigrateReady {

    /**
     * 特别指出：假如 source 库与target 库不在同一个实例，且 source 数据组装依赖 target 库表的数据时，可用如下方式
     */

    @Override
    public String getQuerySql(String target, Long currId, Long idLe, List<Long> groupIdIn, Optional<Long> createdGte) {
        return "SELECT id as t1, LOWER(CONCAT(\n" +
                "   SUBSTR(HEX(guid), 1, 8), '-',\n" +
                "   SUBSTR(HEX(guid), 9, 4), '-',\n" +
                "   SUBSTR(HEX(guid), 13, 4), '-',\n" +
                "   SUBSTR(HEX(guid), 17, 4), '-',\n" +
                "   SUBSTR(HEX(guid), 21)\n" +
                " )) as t2, " +
                " ... " +
                " created as t25, created as t26, group_id as t27" +
                " FROM source.message" +
                " WHERE group_id < 10000000 "
                + (isNotEmpty(groupIdIn) ? "and group_id in"
                                           : (createdGte.isPresent() ? " and created >= " + "CURRENT_TIMESTAMP - INTERVAL " + createdGte.get() + " HOUR"
                                                                     : " and id >= " + currId + " and id <= " + idLe));
    }

    @Override
    public String getExecSql() {
        return "insert ignore into target.message (group_id, message_id, guid,\n" +
                "...\n" +
                "created, updated)\n" +
                "SELECT group_id, ?, ?, \n" +
                " ... " +
                " message_source, " +
                " ?, ?, ?, ?, ?, ?, ?, ?\n" +
                " from target.group where group_id = ?";
    }

    @Override
    public Integer getFieldNum() {
        return 27;
    }

    public Integer getSourceFlag(String target) {
        return 1;
    }

}

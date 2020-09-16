package com.qimok.migrate.ready.clean;

import com.qimok.migrate.ready.IDataMigrateReady;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * @author qimok
 * @description 数据清理准备(为防止死锁，最好根据表的主键进行清理；一定明确要清理的数据范围，以免清错数据)
 * @since 2020-06-1 10:07
 */
@Component
public class MessageCleanReady implements IDataMigrateReady {

    @Override
    public String getQuerySql(String target, Long currId, Long idLe, List<Long> groupIdIn, Optional<Long> createdGte) {
        return "SELECT id as t1 FROM target.message where message_id >= 210000000 and message_id <= 230000000\n" +
                " and message_id >= " + currId + " and message_id <= " + idLe;
    }

    @Override
    public String getExecSql() {
        return "delete from target.message where id = ?";
    }

    @Override
    public Integer getFieldNum() {
        return 1;
    }

}

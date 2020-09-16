package com.qimok.migrate.ready;

import java.util.List;
import java.util.Optional;

/**
 * @author qimok
 * @description 数据迁移准备方法接口类
 * @since 2020-06-1 10:07
 */
public interface IDataMigrateReady {

    /**
     * 获取 查询SQL
     *
     * @param target
     * @param currId
     * @param idLe
     * @param groupIdIn
     * @param createdGte
     * @return
     */
    String getQuerySql(String target, Long currId, Long idLe, List<Long> groupIdIn, Optional<Long> createdGte);

    /**
     * 获取 插入SQL
     *
     * @return
     */
    String getExecSql();

    /**
     * 获取涉及到的字段的数目
     *
     * @return
     */
    default Integer getFieldNum() {
        return 0;
    }

    /**
     * 获取源标识【当 source 库与 target 库在不同数据库实例上时，实现类需要重写此方法】
     * <p>
     * 0：targetConn：目标连接，一般指当前项目的数据库连接
     * 1：sourceConn：数据来源连接
     */
    default Integer getSourceFlag(String target) {
        return 0;
    }

    /**
     * 获取分表配置参数
     * @return
     */
    default SubTableParameters getSubTableParameters() {
        return SubTableParameters.builder()
                .shardingColumn("session_root_id")
                .subTableNum(32)
                .shardingAlgorithm(0)
                .build();
    }

}

package com.qimok.migrate.ready;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SubTableParameters {

    private String execSql;

    private String shardingColumn;

    private Integer subTableNum;

    /**
     * 分片算法
     * <p>
     *     0: 取模分片
     *     1：范围分片
     */
    private Integer shardingAlgorithm;

    private Integer fieldNum;

}

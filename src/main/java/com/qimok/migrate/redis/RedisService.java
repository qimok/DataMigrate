package com.qimok.migrate.redis;

/**
 * @author qimok
 * @since 2020-06-1 10:07
 */
public interface RedisService {

    /**
     * 分布式锁
     */
    Boolean setIfAbsent(String key, String value, Long expired);

    /**
     * 根据key取值
     */
    String getValue(String key);

    /**
     * 删除指定 key
     */
    Boolean delKey(String key);

    /**
     * 获取原来key键对应的值并重新赋新值
     */
    String getAndSet(String key, String value);

    /**
     * 自增并返回，从0开始
     */
    Long incr(String key);

    /**
     * 自减并返回，从指定值开始
     */
    Long decr(String key);

}

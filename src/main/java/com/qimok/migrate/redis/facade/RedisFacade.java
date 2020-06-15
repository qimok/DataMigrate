package com.qimok.migrate.redis.facade;

import com.qimok.migrate.redis.RedisService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class RedisFacade implements RedisService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    public Boolean setIfAbsent(String key, String value, Long expired) {
        log.debug(String.format("lockKey : %s", key));
        // 使用sessionCallBack处理
        SessionCallback<Boolean> sessionCallback = new SessionCallback<Boolean>() {
            @Override
            @SuppressWarnings("unchecked")
            public Boolean execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                redisTemplate.opsForValue().setIfAbsent(key, value);
                redisTemplate.expire(key, expired, TimeUnit.MILLISECONDS);
                List<Object> exec = operations.exec();
                if (exec.size() > 0) {
                    return (Boolean) exec.get(0);
                }
                return false;
            }
        };
        return redisTemplate.execute(sessionCallback);
    }

    @Override
    public String getValue(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    @Override
    public Boolean delKey(String key) {
        return redisTemplate.opsForValue().getOperations().delete(key);
    }

    @Override
    public String getAndSet(String key, String value) {
        return redisTemplate.opsForValue().getAndSet(key, value);
    }

    @Override
    public Long incr(String key) {
        RedisAtomicLong entityIdCounter =
                new RedisAtomicLong(key, Objects.requireNonNull(redisTemplate.getConnectionFactory()));
        return entityIdCounter.getAndIncrement();
    }

    @Override
    public Long decr(String key) {
        RedisAtomicLong entityIdCounter =
                new RedisAtomicLong(key, Objects.requireNonNull(redisTemplate.getConnectionFactory()));
        return entityIdCounter.getAndDecrement();
    }

}

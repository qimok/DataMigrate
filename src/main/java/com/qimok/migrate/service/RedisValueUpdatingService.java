package com.qimok.migrate.service;

import com.qimok.migrate.form.RedisValueUpdateForm;
import com.qimok.migrate.redis.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import static com.qimok.migrate.model.ModelConstants.*;

/**
 * @author qimok
 * @description 主要用于控制数据迁移的 task 和迁移位置 offset
 * @since 2020-06-1 10:07
 */
@Service
public class RedisValueUpdatingService {

    @Autowired
    private RedisService redisService;

    public String updateValueByKey(RedisValueUpdateForm form) throws Exception {
        String key = form.getKey();
        if (key.equals(DATA_MIGRATE_TASK) || key.equals(DATA_MIGRATE_OFFSET)) {
            return redisService.getAndSet(form.getKey(), form.getValue());
        }
        throw new Exception(">>> 输入的 redis 的 key 值不合法，请检查 <<<");
    }

}

package com.qimok.migrate.controller;

import com.qimok.migrate.form.RedisValueUpdateForm;
import com.qimok.migrate.form.DataRepairForm;
import com.qimok.migrate.service.DataRepairingService;
import com.qimok.migrate.service.RedisValueUpdatingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

/**
 * @author qimok
 * @since 2020-06-1 10:07
 */
@RestController
@RequestMapping(produces = APPLICATION_JSON_VALUE)
public class DataMigrateController {

    @Autowired
    private RedisValueUpdatingService redisValueUpdatingService;

    @Autowired
    private DataRepairingService dataRepairingService;

    @PostMapping("/api/redis/value/update/by/key")
    public String updateValueByKey(RedisValueUpdateForm form) throws Exception {
        return redisValueUpdatingService.updateValueByKey(form);
    }

    @PostMapping("/api/data/repair")
    public void repairData(DataRepairForm form) {
        dataRepairingService.repairMessages(form);
    }

}

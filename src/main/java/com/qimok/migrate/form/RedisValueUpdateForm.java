package com.qimok.migrate.form;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiParam;
import lombok.Data;
import lombok.NoArgsConstructor;
import javax.validation.constraints.NotNull;

/**
 * @author qimok
 * @since 2020-06-1 10:07
 */
@Data
@NoArgsConstructor
@ApiModel(value = "数据迁移的redis实体")
public class RedisValueUpdateForm {

    @ApiParam(value = "redis中的key", required = true)
    @NotNull
    private String key;

    @ApiParam(value = "redis中的value", required = true)
    @NotNull
    private String value;

}

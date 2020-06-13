package com.qimok.migrate.form;

import com.sun.istack.internal.NotNull;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiParam;
import lombok.Data;
import lombok.NoArgsConstructor;
import javax.validation.constraints.Size;
import java.util.List;
import java.util.Optional;

/**
 * @author qimok
 * @since 2020-06-1 10:07
 */
@Data
@NoArgsConstructor
@ApiModel(value = "精确/增量迁移消息")
public class DataRepairForm {

    @ApiParam(value = "消息组id", required = true)
    @NotNull
    @Size(min = 1)
    private List<Long> groupIdIn;

    @ApiParam(value = "创建时间大于等于几个小时")
    private Optional<Long> createdGte = Optional.empty();

    @ApiParam(value = "修复标识", required = true)
    @NotNull
    @Size(min = 1)
    private List<Integer> repairFlagIn;

}

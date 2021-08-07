package io.mycat.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jamie12221
 * date 2019-05-03 14:54
 **/
@Data
public class SharingFuntionRootConfig {

    List<ShardingFunction> functions = new ArrayList<>();


}

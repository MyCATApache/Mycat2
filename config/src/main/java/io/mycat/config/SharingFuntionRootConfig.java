package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 * date 2019-05-03 14:54
 **/
@Data
public class SharingFuntionRootConfig {

    List<ShardingFuntion> functions = new ArrayList<>();



}

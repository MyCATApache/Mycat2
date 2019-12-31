package io.mycat.config;

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

    @Data
    @Builder
    public static class ShardingFuntion implements Cloneable {
        String name;
        String clazz;
        Map<String, String> properties = new HashMap<>();
        Map<String, String> ranges = new HashMap<>();

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }

}

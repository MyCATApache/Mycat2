package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
public class ShardingFuntion {
    String name;
    String clazz;
    Map<String, Object> properties = new HashMap<>();
    Map<String, Object> ranges = new HashMap<>();

    public ShardingFuntion() {
    }
}
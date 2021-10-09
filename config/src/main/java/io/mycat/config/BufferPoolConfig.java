package io.mycat.config;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class BufferPoolConfig {
    private static final Map<String, Object> defaultConfig = defaultValue();
    Map<String, Object> args;

    public static Map defaultValue() {
        HashMap defaultConfig = new HashMap<>();
        long pageSize = 1024 * 1024 * 2;
        defaultConfig.put("pageSize", pageSize);
        defaultConfig.put("chunkSize", 8192 / 2);
        defaultConfig.put("pageCount", 8);
        return defaultConfig;
    }
}

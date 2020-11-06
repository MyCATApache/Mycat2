package io.mycat.config;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class BufferPoolConfig {
    String poolName;
    Map<String, String> args = new HashMap<>();
}

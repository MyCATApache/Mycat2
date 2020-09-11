package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

@Data
@EqualsAndHashCode
public final class LogicSchemaConfig {
    String schemaName;
    String targetName;
    Generator generator;
    Map<String, ShardingTableConfig> shadingTables = new HashMap<>();
    Map<String, GlobalTableConfig> globalTables = new HashMap<>();
    Map<String, NormalTableConfig> normalTables = new HashMap<>();
    Map<String, CustomTableConfig> customTables = new HashMap<>();
}

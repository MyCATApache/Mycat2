package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Data
@EqualsAndHashCode
public final class LogicSchemaConfig {
    String schemaName;
    String targetName;
    Map<String, ShardingTableConfig> shardingTables = new HashMap<>();
    Map<String, GlobalTableConfig> globalTables = new HashMap<>();
    Map<String, NormalTableConfig> normalTables = new HashMap<>();
    Map<String, CustomTableConfig> customTables = new HashMap<>();
    Map<String, ViewConfig> views = new HashMap<>();

    public Optional<Object> findTable(String tableName){
        ShardingTableConfig shardingTableConfig = shardingTables.get(tableName);
        if (shardingTableConfig != null){
            return  Optional.of(shardingTableConfig);
        }
        GlobalTableConfig globalTableConfig = globalTables.get(tableName);
        if (globalTableConfig != null){
            return Optional.of(globalTableConfig);
        }
        NormalTableConfig normalTableConfig = normalTables.get(tableName);
        if (normalTableConfig != null){
            return Optional.of(normalTableConfig);
        }
        return Optional.empty();
    }
}

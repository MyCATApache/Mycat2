package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@AllArgsConstructor
@Data
@Builder
@EqualsAndHashCode
public class ShardingTableConfig {
    ShardingBackEndTableInfoConfig partition;
    ShardingFuntion function;
    String createTableSQL;

    Map<String,ShardingTableConfig> shardingIndexTables;

    public ShardingTableConfig() {
    }
}
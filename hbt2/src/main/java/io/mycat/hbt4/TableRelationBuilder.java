package io.mycat.hbt4;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class TableRelationBuilder {
    HashMap<ShardingInfo, Set<SchemaInfo>> builder = new HashMap<>();

    public void add(String schemaName, String tableName, ShardingInfo shardingInfo) {
        Set<SchemaInfo> set = builder.computeIfAbsent(shardingInfo, shardingInfo1 -> new HashSet<>());
        set.add(new SchemaInfo(schemaName, tableName));
    }

    public TableRelationChecker build() {
        return (shardingInfo, schemaName, tableName) -> {
            Set<SchemaInfo> schemaInfos = builder.get(shardingInfo);
            if (schemaInfos == null) {
                return false;
            }
            return schemaInfos.contains(new SchemaInfo(schemaName, tableName));
        };
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    public static class SchemaInfo {
        final String targetSchema;
        final String targetTable;
    }
}
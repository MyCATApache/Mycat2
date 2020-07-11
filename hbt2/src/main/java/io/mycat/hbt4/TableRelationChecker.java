package io.mycat.hbt4;

public interface TableRelationChecker {

    public boolean check(ShardingInfo shardingInfo, String schemaName, String tableName);
}
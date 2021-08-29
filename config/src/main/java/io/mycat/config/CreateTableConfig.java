package io.mycat.config;

import lombok.Data;

@Data
public class CreateTableConfig {
    private String schemaName;
    private String tableName;
    private ShardingTableConfig shardingTable;
    private GlobalTableConfig globalTable;
    private NormalTableConfig normalTable;
}

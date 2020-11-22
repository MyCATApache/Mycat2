package io.mycat.config;

import lombok.Data;

@Data
public class CreateTableConfig {
    private String schemaName;
    private String tableName;
    private ShardingTableConfig shadingTable;
    private GlobalTableConfig globalTable;
    private NormalTableConfig normalTable;
}

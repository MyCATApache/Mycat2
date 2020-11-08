package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@AllArgsConstructor
@Data
@Builder
@EqualsAndHashCode
public  class ShardingBackEndTableInfoConfig {
    private String targetNames;
    private String schemaNames;
    private String tableNames;

    public ShardingBackEndTableInfoConfig() {
    }
}

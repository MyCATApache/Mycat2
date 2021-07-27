package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@AllArgsConstructor
@Data
@Builder
@EqualsAndHashCode
public class ShardingBackEndTableInfoConfig {
    private String targetNames;
    private String schemaNames;
    private String tableNames;

    private List<List> data;

    public ShardingBackEndTableInfoConfig() {
    }
}

package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@AllArgsConstructor
@Data
@Builder
@EqualsAndHashCode
public class NormalBackEndTableInfoConfig {
    private String targetName;
    private String schemaName;
    private String tableName;

    public NormalBackEndTableInfoConfig() {

    }
}
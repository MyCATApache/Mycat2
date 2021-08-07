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
    @javax.validation.constraints.NotNull
    private String targetName;
    @javax.validation.constraints.NotNull
    private String schemaName;
    @javax.validation.constraints.NotNull
    private String tableName;

    public NormalBackEndTableInfoConfig() {

    }
}
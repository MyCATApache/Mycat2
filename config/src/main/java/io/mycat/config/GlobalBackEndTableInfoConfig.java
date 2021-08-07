package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@AllArgsConstructor
@Data
@Builder
@EqualsAndHashCode
public class GlobalBackEndTableInfoConfig {
    @javax.validation.constraints.NotNull
    private String targetName;

    public GlobalBackEndTableInfoConfig() {
    }
}
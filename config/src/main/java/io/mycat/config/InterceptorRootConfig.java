package io.mycat.config;

import lombok.Data;

import java.util.List;

@Data
public class InterceptorRootConfig {
    List<PatternRootConfig> patternRootConfigs;

    public InterceptorRootConfig() {
    }
}
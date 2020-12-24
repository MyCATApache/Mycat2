package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode
public class DatasourceRootConfig {
    private String datasourceProviderClass;
    private List<DatasourceConfig> datasources = new ArrayList<>();
    private TimerConfig timer = new TimerConfig();
}
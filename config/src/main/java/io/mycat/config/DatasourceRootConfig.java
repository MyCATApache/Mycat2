package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Data
@EqualsAndHashCode
public class DatasourceRootConfig {
    private String datasourceProviderClass;
    private List<DatasourceConfig> datasources = new ArrayList<>();
    private TimerConfig timer = new TimerConfig();



}
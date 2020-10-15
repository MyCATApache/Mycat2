package io.mycat.config;

import io.mycat.config.*;
import io.mycat.datasource.jdbc.datasourceprovider.DruidDatasourceProvider;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

@Data
@EqualsAndHashCode
public class MycatServerConfig {
    LoadBalance loadBalance = new LoadBalance();
    ServerConfig server = new io.mycat.config.ServerConfig ();
    ManagerConfig manager = new ManagerConfig();
    String mode = "local";
    String datasourceProvider = DruidDatasourceProvider.class.getName();
    Map<String, Object> properties;
    List<SequenceConfig> sequences;
}
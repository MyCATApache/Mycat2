package io.mycat.ui;

import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.LogicSchemaConfig;

import java.util.List;
import java.util.Optional;

public interface InfoProvider {
    List<LogicSchemaConfig> schemas();

    List<ClusterConfig> clusters();

    List<DatasourceConfig> datasources();

    public Optional<LogicSchemaConfig> getSchemaConfigByName(String schemaName) ;

    Optional<DatasourceConfig> getDatasourceConfigByPath(String path);

    Optional<ClusterConfig> getClusterConfigByPath(String path);

    String translate(String name);

}
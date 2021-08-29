package io.mycat.config.controller;

import io.mycat.MetaClusterCurrent;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.DatasourceConfigProvider;
import io.mycat.datasource.jdbc.DruidDatasourceProvider;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.replica.ReplicaSelectorManager;

import java.util.Map;

public class JdbcManagerController {
    public static void update(Map<String, DatasourceConfig> datasourceConfigMap) {
        if (MetaClusterCurrent.exist(JdbcConnectionManager.class)) {
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            jdbcConnectionManager.close();
        }
        JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(
                DruidDatasourceProvider.class.getName(),
                datasourceConfigMap);
        MetaClusterCurrent.register(JdbcConnectionManager.class, jdbcConnectionManager);
        jdbcConnectionManager.registerReplicaSelector(MetaClusterCurrent.wrapper(ReplicaSelectorManager.class));
        DatasourceConfigProvider datasourceConfigProvider = new DatasourceConfigProvider() {
            @Override
            public Map<String, DatasourceConfig> get() {
                return datasourceConfigMap;
            }
        };
        MetaClusterCurrent.register(DatasourceConfigProvider.class, datasourceConfigProvider);
    }

    public static void addDatasource(DatasourceConfig config) {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        jdbcConnectionManager.addDatasource(config);
        jdbcConnectionManager.registerReplicaSelector(MetaClusterCurrent.wrapper(ReplicaSelectorManager.class));
        Map<String, DatasourceConfig> configMap = jdbcConnectionManager.getConfig();
        DatasourceConfigProvider datasourceConfigProvider = new DatasourceConfigProvider() {
            @Override
            public Map<String, DatasourceConfig> get() {
                return configMap;
            }
        };
        MetaClusterCurrent.register(DatasourceConfigProvider.class, datasourceConfigProvider);
    }

    public static void removeDatasource(String name) {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        jdbcConnectionManager.removeDatasource(name);
        jdbcConnectionManager.registerReplicaSelector(MetaClusterCurrent.wrapper(ReplicaSelectorManager.class));
        Map<String, DatasourceConfig> configMap = jdbcConnectionManager.getConfig();
        DatasourceConfigProvider datasourceConfigProvider = new DatasourceConfigProvider() {
            @Override
            public Map<String, DatasourceConfig> get() {
                return configMap;
            }
        };
        MetaClusterCurrent.register(DatasourceConfigProvider.class, datasourceConfigProvider);
    }
}
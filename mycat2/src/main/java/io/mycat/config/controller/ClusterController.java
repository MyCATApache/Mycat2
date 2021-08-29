package io.mycat.config.controller;

import io.mycat.MetaClusterCurrent;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.replica.ReplicaSelectorManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.mycat.config.CoreMetadataStorageManager.createReplicaSelector;

public class ClusterController {
    public static void update(List<ClusterConfig> clusterConfigs, Map<String, DatasourceConfig> datasourceConfigMap) {
        if (MetaClusterCurrent.exist(ReplicaSelectorManager.class)) {
            MetaClusterCurrent.wrapper(ReplicaSelectorManager.class).close();
        }
        ReplicaSelectorManager replicaSelector = createReplicaSelector(clusterConfigs, MetaClusterCurrent.wrapper(LoadBalanceManager.class), datasourceConfigMap);
        MetaClusterCurrent.register(ReplicaSelectorManager.class, replicaSelector);
        if (MetaClusterCurrent.exist(JdbcConnectionManager.class)) {
            MetaClusterCurrent.wrapper(JdbcConnectionManager.class).registerReplicaSelector(replicaSelector);
        }
    }

    public static void addCluster(ClusterConfig clusterConfig){
        ReplicaSelectorManager originalReplicaSelector = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
        List<ClusterConfig> config = new ArrayList<>(originalReplicaSelector.getConfig());
        config.removeIf(c -> c.getName().equalsIgnoreCase(clusterConfig.getName()));
        config.add(clusterConfig);
        update(config, MetaClusterCurrent.wrapper(JdbcConnectionManager.class).getConfig());
    }
    public static void removeCluster(String name){
        ReplicaSelectorManager originalReplicaSelector = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
        List<ClusterConfig> config = new ArrayList<>(originalReplicaSelector.getConfig());
        config.removeIf(c -> c.getName().equalsIgnoreCase(name));
        update(config, MetaClusterCurrent.wrapper(JdbcConnectionManager.class).getConfig());
    }
}
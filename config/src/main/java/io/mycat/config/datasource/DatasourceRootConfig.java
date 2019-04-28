package io.mycat.config.datasource;

import io.mycat.config.Configurable;

import java.util.List;

/**
 * Desc: 用于加载datasource.yml的类
 *
 * @date: 10/09/2017
 * @author: gaozhiwen
 */
public class DatasourceRootConfig implements Configurable {
    private List<ReplicaConfig> replicas;

    public List<ReplicaConfig> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<ReplicaConfig> replicas) {
        this.replicas = replicas;
    }
}

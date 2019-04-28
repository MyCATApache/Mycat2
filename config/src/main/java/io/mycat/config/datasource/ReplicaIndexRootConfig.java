package io.mycat.config.datasource;


import io.mycat.config.Configurable;

import java.util.Map;

/**
 * Desc: 加载replica-index.yml文件配置
 *
 * @date: 10/09/2017
 * @author: gaozhiwen
 */
public class ReplicaIndexRootConfig implements Configurable {
    private Map<String, Integer> replicaIndexes;

    public Map<String, Integer> getReplicaIndexes() {
        return replicaIndexes;
    }

    public void setReplicaIndexes(Map<String, Integer> replicaIndexes) {
        this.replicaIndexes = replicaIndexes;
    }
}

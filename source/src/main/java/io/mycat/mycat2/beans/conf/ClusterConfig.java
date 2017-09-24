package io.mycat.mycat2.beans.conf;

import io.mycat.proxy.Configurable;

/**
 * Desc: 对应mycat.yml文件，集群配置
 *
 * @date: 19/09/2017
 * @author: gaozhiwen
 */
public class ClusterConfig implements Configurable {
    private ClusterBean cluster;

    public ClusterBean getCluster() {
        return cluster;
    }

    public void setCluster(ClusterBean cluster) {
        this.cluster = cluster;
    }
}

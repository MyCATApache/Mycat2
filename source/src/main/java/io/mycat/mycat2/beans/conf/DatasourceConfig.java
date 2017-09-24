package io.mycat.mycat2.beans.conf;

import io.mycat.mycat2.beans.GlobalBean;
import io.mycat.proxy.Configurable;

import java.util.List;

/**
 * Desc: 用于加载datasource.yml的类
 *
 * @date: 10/09/2017
 * @author: gaozhiwen
 */
public class DatasourceConfig implements Configurable {
    private List<ReplicaBean> replicas;

    public List<ReplicaBean> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<ReplicaBean> replicas) {
        this.replicas = replicas;
    }
}

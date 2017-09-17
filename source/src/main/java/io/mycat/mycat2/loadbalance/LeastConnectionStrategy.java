package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.ClusterNode;

import java.util.Collection;

/**
 * 最少连接的均衡策略
 *
 * Created by ynfeng on 2017/9/12.
 */
public class LeastConnectionStrategy implements LoadBalanceStrategy{
    @Override
    public ClusterNode getNode(Collection<ClusterNode> allNode, Object attachement) {
        return null;
    }
}

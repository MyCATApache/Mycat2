package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.ClusterNode;

import java.util.Collection;

/**
 * 轮询策略
 *
 * Created by ynfeng on 2017/9/12.
 */
public class RoundRobinStrategy implements LoadBalanceStrategy{
    @Override
    public ClusterNode getNode(Collection<ClusterNode> allNode, Object attachement) {
        return null;
    }
}

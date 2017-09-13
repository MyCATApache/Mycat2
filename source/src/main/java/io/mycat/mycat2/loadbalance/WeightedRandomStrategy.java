package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.ClusterNode;

import java.util.Collection;

/**
 * 带权重的随机轮询策略
 * Created by ynfeng on 2017/9/12.
 */
public class WeightedRandomStrategy implements LoadBalanceStrategy{
    @Override
    public ClusterNode getNode(Collection<ClusterNode> allNode, Object attachement) {
        return null;
    }
}

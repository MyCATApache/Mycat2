package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.ClusterNode;

import java.util.Collection;

/**
 * 随机轮询
 *
 * Created by ynfeng on 2017/9/12.
 */
public class RandomStrategy implements LoadBalanceStrategy{
    @Override
    public ClusterNode getNode(Collection<ClusterNode> allNode, Object attachement) {
        return null;
    }
}

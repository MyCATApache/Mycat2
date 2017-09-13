package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.ClusterNode;

import java.util.Collection;

/**
 * 基于平均响应时间的均衡策略
 *
 * Created by ynfeng on 2017/9/12.
 */
public class ResponseTimeStrategy implements LoadBalanceStrategy {
    @Override
    public ClusterNode getNode(Collection<ClusterNode> allNode, Object attachement) {
        return null;
    }
}

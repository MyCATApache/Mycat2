package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.ClusterNode;

import java.util.Collection;

/**
 * 均衡策略接口
 * <p>
 * Created by ynfeng on 2017/9/12.
 */
public interface LoadBalanceStrategy {

    /**
     * 按照策略获取一个节点
     *
     * @param allNode 集群节点列表
     * @param attachement 附加参数
     *
     * @return 节点
     */
    ClusterNode getNode(Collection<ClusterNode> allNode, Object attachement);
}

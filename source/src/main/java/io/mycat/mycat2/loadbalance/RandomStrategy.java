package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.ClusterNode.NodeState;
import io.mycat.proxy.man.MyCluster.ClusterState;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * 随机轮询
 * <p>
 * Created by ynfeng on 2017/9/12.
 */
public class RandomStrategy implements LoadBalanceStrategy {
    @Override
    public ClusterNode getNode(Collection<ClusterNode> allNode, Object attachement) {
        ProxyRuntime proxyRuntime = ProxyRuntime.INSTANCE;
        String myNodeId = proxyRuntime.getMyCLuster().getMyNodeId();
        List<ClusterNode> nodeList = allNode.stream().filter(clusterNode ->
                                                                     clusterNode.getState() != null &&
                                                                     !myNodeId.equals(clusterNode.id) &&
                                                                     NodeState.Online == clusterNode.getState()
        ).collect(Collectors.toList());
        if (nodeList.size() == 1) {
            return nodeList.get(0);
        } else {
            int random = ThreadLocalRandom.current().nextInt(0, nodeList.size());
            return nodeList.get(random);
        }
    }
}

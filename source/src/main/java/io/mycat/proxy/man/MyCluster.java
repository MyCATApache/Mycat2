package io.mycat.proxy.man;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.man.ClusterNode.NodeState;

/**
 * Mycat集群，保存了当前节点成员以及状态
 * 
 * @author wuzhihui
 *
 */
public class MyCluster {

	private static Logger logger = LoggerFactory.getLogger(MyCluster.class);

	enum ClusterState {
		LeaderElection, FullClustered, PartClustered, SpiltFailed
	};

	private ConcurrentHashMap<String, ClusterNode> allNodes = new ConcurrentHashMap<>();
	private final ClusterNode myNode;

	private final Selector nioSelector;

	public MyCluster(Selector nioSelector, ClusterNode myNode, ArrayList<ClusterNode> allClusterNodes) {
		this.myNode = myNode;
		this.nioSelector = nioSelector;
		for (ClusterNode node : allClusterNodes) {
			allNodes.put(node.id, node);
		}

	}

	public void onClusterNodeUp(String nodeId, long sysStartTime) {

		ClusterNode theNode = allNodes.get(nodeId);
		theNode.setNodeStartTime(sysStartTime);
		theNode.setState(NodeState.Online);
		logger.info("node online " + theNode.id + " at " + theNode.ip + ":" + theNode.port + " started at "
				+ new Timestamp(theNode.getNodeStartTime()));

	}

	public void onClusterNodeDown(String nodeId) {
		ClusterNode theNode = allNodes.get(nodeId);
		theNode.setState(NodeState.Offline);
		logger.info("node offline " + theNode.id + " at " + theNode.ip + ":" + theNode.port + " started at "
				+ new Timestamp(theNode.getNodeStartTime()));

	}

	public ClusterNode findNode(String nodeId) {
		return allNodes.get(nodeId);
	}

	public ClusterNode getMyNode() {
		return myNode;
	}

	/**
	 * 初始化集群
	 */
	public void initCluster() {
		ClusterNode[] nodes = new ClusterNode[allNodes.size()];
		allNodes.values().toArray(nodes);
		Arrays.sort(nodes);
		// 从最后开始便利，找到比自己小的节点，尝试连接
		for (ClusterNode curNode : nodes) {
			if (curNode.equals(myNode))
				continue;
			try {
				logger.info("Connecting to MycatNode " + curNode);
				InetSocketAddress serverAddress = new InetSocketAddress(curNode.ip, curNode.port);
				SocketChannel socketChannel = SocketChannel.open();
				socketChannel.configureBlocking(false);
				socketChannel.connect(serverAddress);
				socketChannel.register(nioSelector, SelectionKey.OP_CONNECT);
			} catch (Exception e) {
				logger.warn("connect err " + e);
			}

		}

	}

}

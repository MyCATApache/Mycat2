package io.mycat.proxy.man;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import io.mycat.proxy.ConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.Session;
import io.mycat.proxy.man.ClusterNode.NodeState;
import io.mycat.proxy.man.packet.JoinCLusterReqPacket;
import io.mycat.proxy.man.packet.JoinCLusterNotifyPacket;
import io.mycat.proxy.man.packet.NodeRegInfoPacket;

/**
 * Mycat集群，保存了当前节点成员以及状态
 * 
 * @author wuzhihui
 *
 */
public class MyCluster {

	private static Logger logger = LoggerFactory.getLogger(MyCluster.class);

	public enum ClusterState {
		Joining(0), LeaderElection(1), Clustered(2);
		private byte stateCode;

		private ClusterState(int stateCode) {
			this.stateCode = (byte) stateCode;
		}

		public static ClusterState getState(byte stateCode) {
			return ClusterState.values()[stateCode];
		}

		public byte getSateCode() {
			return this.stateCode;
		}

	};

	private ConcurrentHashMap<String, ClusterNode> allNodes = new ConcurrentHashMap<>();
	private final ClusterNode myNode;
	private ClusterNode myLeader;
	private ClusterState clusterState = ClusterState.Joining;
	private long lastClusterStateTime;
	private final Selector nioSelector;

	public MyCluster(Selector nioSelector, ClusterNode myNode, ArrayList<ClusterNode> allClusterNodes) {
		this.myNode = myNode;
		this.nioSelector = nioSelector;
		for (ClusterNode node : allClusterNodes) {
			allNodes.put(node.id, node);
		}

	}

	/**
	 * 初始化集群
	 */
	public void initCluster() {
		ClusterNode[] nodes = new ClusterNode[allNodes.size()];
		allNodes.values().toArray(nodes);
		// 排序结果，节点最大的在数组第一个位置
		Arrays.sort(nodes);
		for (ClusterNode curNode : nodes) {
			if (curNode.equals(myNode))
				continue;
			try {
				logger.info("Connecting to MycatNode " + curNode);
				InetSocketAddress serverAddress = new InetSocketAddress(curNode.ip, curNode.port);
				SocketChannel socketChannel = SocketChannel.open();
				socketChannel.configureBlocking(false);
				socketChannel.register(nioSelector, SelectionKey.OP_CONNECT, curNode.id);
				socketChannel.connect(serverAddress);
			} catch (Exception e) {
				logger.warn("connect err " + e);
			}
		}
	}

	/**
	 * 发送心跳包到已有节点，并且对连接失败的节点重新发起连接，以维持集群状态
	 */
	public void heatbeateAndReconnectNodes() {

	}

	public void onClusterNodeUp(NodeRegInfoPacket pkg, AdminSession session) throws IOException {
		String theNodeId = pkg.getNodeId();
		ClusterNode theNode = allNodes.get(theNodeId);
		theNode.setNodeStartTime(pkg.getStartupTime());
		theNode.setState(NodeState.Online);
		theNode.setMyLeaderId(pkg.getMyLeader());
		theNode.setMyClusterState(pkg.getClusterState(), pkg.getLastClusterStateTime());
		logger.info("Node online " + theNode.id + " at " + theNode.ip + ":" + theNode.port + " started at "
				+ new Timestamp(theNode.getNodeStartTime()) + " cluster leader " + theNode.getMyLeaderId()
				+ " cluster state " + theNode.getMyClusterState() + " cluster time:"
				+ theNode.getLastClusterStateTime());
		if (clusterState == ClusterState.Joining || clusterState == ClusterState.LeaderElection) {
			if (theNode.getMyClusterState() == ClusterState.Clustered) {
				myLeader = this.findNode(theNode.getMyLeaderId());
				if (myLeader.getState() == NodeState.Online) {
					logger.info("send join cluster message to leader " + theNode.id);
					// 向Leader发送请求加入集群的报文
					JoinCLusterReqPacket joinPkg = new JoinCLusterReqPacket(getMyAliveNodes());
					findSession(myLeader.id).answerClientNow(joinPkg);
					return;
				}

			} else if (getMyAliveNodesCount() >= allNodes.size() / 2 && imSmallestAliveNode()) {
				// 是连接中当前编号最小的节点，当选为Leader
				logger.info("I'm smallest alive node ,and exceeded 1/2 nodes alive ,so I'm the King now !");
				this.setClusterState(ClusterState.Clustered);
				this.myLeader = this.myNode;
				JoinCLusterNotifyPacket joinReps = createJoinNotifyPkg(session,JoinCLusterNotifyPacket.JOIN_STATE_NEED_ACK);
				notifyAllNodes(session,joinReps);

			} else if (theNode.getMyClusterState() == ClusterState.LeaderElection) {
				setClusterState(ClusterState.LeaderElection);
			}
		} else if (myLeader == myNode) {// 当前是集群状态，并且自己是Leader，就回话，可以加入集群
			session.answerClientNow(createJoinNotifyPkg(session,JoinCLusterNotifyPacket.JOIN_STATE_NEED_ACK));
		}

	}

	private void notifyAllNodes(AdminSession session,ManagePacket packet) 
	{
		for (Session theSession : session.getMySessionManager().getAllSessions()) {
			AdminSession nodeSession = (AdminSession) theSession;
			if (!nodeSession.isChannelOpen()) {
				try
				{
				nodeSession.answerClientNow(packet);
				}catch(Exception e)
				{
					logger.warn("notify node err "+nodeSession.getNodeId(),e);
				}
			}

		}
	}
	private JoinCLusterNotifyPacket createJoinNotifyPkg(AdminSession session,byte joinState) {
		JoinCLusterNotifyPacket respPacket = new JoinCLusterNotifyPacket(session.cluster().getMyAliveNodes(),
				ProxyRuntime.INSTANCE.getProxyConfig().getConfigVersion(ConfigKey.MYCAT_CONF));
		respPacket.setJoinState(joinState);
		return respPacket;
	}

	private boolean imSmallestAliveNode() {
		String myId = this.getMyNodeId();
		for (ClusterNode curNode : this.allNodes.values()) {
			if (curNode.getState() == NodeState.Online) {
				if (myId.compareTo(curNode.id) > 0) {
					return false;
				}
			}
		}
		return true;
	}

	public String getMyAliveNodes() {
		StringBuilder sb = new StringBuilder();
		for (ClusterNode curNode : this.allNodes.values()) {
			if (curNode.getState() == NodeState.Online) {
				sb.append(curNode.id).append(",");
			}
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}

	private int getMyAliveNodesCount() {
		int aliveCount = 1;
		for (ClusterNode curNode : this.allNodes.values()) {
			if (curNode.getState() == NodeState.Online) {
				aliveCount++;
			}
		}
		return aliveCount;
	}

	private AdminSession findSession(String nodeId) {
		for (AdminSession session : ProxyRuntime.INSTANCE.getAdminSessionManager().getAllSessions()) {
			if (session.getNodeId().equals(nodeId)) {
				return session;
			}
		}
		return null;
	}

	public void onClusterNodeDown(String nodeId, AdminSession session)  {
		ClusterNode theNode = allNodes.get(nodeId);
		theNode.setState(NodeState.Offline);
		logger.info("Node offline " + theNode.id + " at " + theNode.ip + ":" + theNode.port + " started at "
				+ new Timestamp(theNode.getNodeStartTime()));
		if (theNode == myLeader) {
			logger.warn("Leader crashed " + myLeader.id + ' ' + this.getMyNodeId() + ",enter Leader election state ");
			this.setClusterState(ClusterState.LeaderElection);
		} else if (myLeader == myNode) {
               //如果集群数量小于1/2就通知集群解散
			   if(this.getMyAliveNodesCount()<this.allNodes.size()/2)
			   {
				   logger.warn("Less then 1/2 mumbers  in my Kingdom ,so I quit ");
				   this.setClusterState(ClusterState.LeaderElection);
				   this.myLeader=null;
				   JoinCLusterNotifyPacket joinReps = createJoinNotifyPkg(session,JoinCLusterNotifyPacket.JOIN_STATE_DENNIED);
					notifyAllNodes(session,joinReps);
			   }
		}

	}

	public String getMyNodeId() {
		return this.myNode.id;
	}

	public ClusterNode getMyLeader() {
		return myLeader;
	}

	public String getMyLeaderId() {
		return (myLeader == null) ? null : myLeader.id;
	}

	public ClusterNode findNode(String nodeId) {
		return allNodes.get(nodeId);
	}

	public ClusterNode getMyNode() {
		return myNode;
	}

	public void setMyLeader(ClusterNode myLeader) {
		this.myLeader = myLeader;
	}

	public ClusterState getClusterState() {
		return clusterState;
	}

	public long getLastClusterStateTime() {
		return lastClusterStateTime;
	}

	public void setClusterState(ClusterState clusterState) {
		this.clusterState = clusterState;
		this.lastClusterStateTime = System.currentTimeMillis();
	}

}

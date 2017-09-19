package io.mycat.proxy.man;

import java.util.ArrayList;

import io.mycat.proxy.man.MyCluster.ClusterState;

/**
 * 集群节点,id必须全局唯一
 * 
 * @author wuzhihui
 *
 */
public class ClusterNode implements Comparable<ClusterNode> {
	public enum NodeState {
		Online, Offline;
	}

	//myNodeId，集群中的唯一标识
	public String id;
	//集群的ip，clusterIP
	public String ip;
	//集群的port，clusterPort
	public int port;
	private long lastStateTime;
	private long nodeStartTime;
	private NodeState state;
	private String myLeaderId;
	private ClusterState myClusterState;
	private long lastClusterStateTime;
	//mycat的port
	public int proxyPort;

	public ClusterNode(String id, String ip, int port) {
		super();
		this.id = id;
		this.ip = ip;
		this.port = port;
	}

	public static ArrayList<ClusterNode> parseNodesInf(String allNodes) {
		ArrayList<ClusterNode> result = new ArrayList<>();
		String[] items = allNodes.split(",");
		for (String item : items) {
			String[] nodeInfs = item.split(":");
			result.add(new ClusterNode(nodeInfs[0], nodeInfs[1], Integer.valueOf(nodeInfs[2])));
		}
		return result;
	}

	public NodeState getState() {
		return state;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getMyLeaderId() {
		return myLeaderId;
	}

	public void setMyLeaderId(String myLeaderId) {
		this.myLeaderId = myLeaderId;
	}

	public long getLastClusterStateTime() {
		return lastClusterStateTime;
	}

	public void setState(NodeState state) {
		lastStateTime = System.currentTimeMillis();
		this.state = state;
	}

	public long getLastStateTime() {
		return lastStateTime;
	}

	
	public ClusterState getMyClusterState() {
		return myClusterState;
	}

	public void setMyClusterState(ClusterState myClusterState,long clusterStateTime) {
		this.myClusterState = myClusterState;
		this.lastClusterStateTime=clusterStateTime;
	}

	public long getNodeStartTime() {
		return nodeStartTime;
	}

	public void setNodeStartTime(long nodeStartTime) {
		this.nodeStartTime = nodeStartTime;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ClusterNode other = (ClusterNode) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public int compareTo(ClusterNode o) {
		// 排序从大到小，所以反着调用
		return o.id.compareTo(this.id);
	}

	@Override
	public String toString() {
		return "ClusterNode [id=" + id + ", ip=" + ip + ", port=" + port + ", lastStateTime=" + lastStateTime
				+ ", nodeStartTime=" + nodeStartTime + ", state=" + state + "]";
	}

}

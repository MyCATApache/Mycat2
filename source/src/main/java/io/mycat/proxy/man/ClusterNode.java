package io.mycat.proxy.man;

import java.util.ArrayList;

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

	public String id;
	public String ip;
	public int port;
	private long lastStateTime;
	private long nodeStartTime;
	private volatile NodeState state;

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

	public void setState(NodeState state) {
		lastStateTime = System.currentTimeMillis();
		this.state = state;
	}

	public long getLastStateTime() {
		return lastStateTime;
	}

	public void setLastStateTime(long lastStateTime) {
		this.lastStateTime = lastStateTime;
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

package io.mycat.proxy;

import java.util.HashMap;
import java.util.Map;

/**
 * 代理的配置信息
 *
 * @author wuzhihui
 *
 */
public class ProxyConfig implements Configurable {
	// 绑定的数据传输IP地址
	private String bindIP = "0.0.0.0";
	// 绑定的数据传输端口
	private int bindPort = 8066;
	private boolean clusterEnable = false;
	// 是否开启负载均衡
	private boolean loadBalanceEnable = false;
	// 负载均衡绑定的ip
	private String loadBalanceIp = "0.0.0.0";
	// 负载均衡缓定的端口
	private int loadBalancePort = 9088;
	// 集群通信绑定的IP地址
	private String clusterIP = "0.0.0.0";
	// 集群通信绑定的端口
	private int clusterPort = 9066;
	private String myNodeId;
	// 逗号分隔的所有集群节点的ID:IP:Port信息，如
	// leader-1:127.0.0.1:9066,leader-2:127.0.0.1:9068,leader-3:127.0.0.1:9069
	private String allNodeInfs;

	// 默认空闲超时时间
	private long idleTimeout = 30 * 60 * 1000L;
	// 默认复制组 空闲检查周期
	private long replicaIdleCheckPeriod = 5 * 60 * 1000L;
	// 默认复制组心跳周期
	private long replicaHeartbeatPeriod = 10 * 1000L;

	private int timerExecutor = 2;

	// sql execute timeout (second)
	private long sqlExecuteTimeout = 300;
	private long processorCheckPeriod = 1 * 1000L;

	private long minSwitchtimeInterval = 30 * 60 * 1000L;  //默认三十分钟
	// 用于集群中发送prepare报文等待确认的时间，超时则认为失败，默认30s
	private int prepareDelaySeconds = 30;

	public ProxyConfig() {}

	public String getBindIP() {
		return bindIP;
	}

	public void setBindIP(String bindIP) {
		this.bindIP = bindIP;
	}

	public int getBindPort() {
		return bindPort;
	}

	public void setBindPort(int bindPort) {
		this.bindPort = bindPort;
	}

	public boolean isClusterEnable() {
		return clusterEnable;
	}

	public void setClusterEnable(boolean clusterEnable) {
		this.clusterEnable = clusterEnable;
	}

	public String getMyNodeId() {
		return myNodeId;
	}

	public void setMyNodeId(String myNodeId) {
		this.myNodeId = myNodeId;
	}

	public String getAllNodeInfs() {
		return allNodeInfs;
	}

	public void setAllNodeInfs(String allNodeInfs) {
		this.allNodeInfs = allNodeInfs;
	}

	public String getClusterIP() {
		return (clusterIP != null) ? clusterIP : this.bindIP;
	}

	public void setClusterIP(String clusterIP) {
		this.clusterIP = clusterIP;
	}

	public int getClusterPort() {
		return clusterPort;
	}

	public void setClusterPort(int clusterPort) {
		this.clusterPort = clusterPort;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public int getTimerExecutor() {
		return timerExecutor;
	}

	public void setTimerExecutor(int timerExecutor) {
		this.timerExecutor = timerExecutor;
	}

	public long getSqlExecuteTimeout() {
		return sqlExecuteTimeout;
	}

	public void setSqlExecuteTimeout(long sqlExecuteTimeout) {
		this.sqlExecuteTimeout = sqlExecuteTimeout;
	}

	public long getProcessorCheckPeriod() {
		return processorCheckPeriod;
	}

	public void setProcessorCheckPeriod(long processorCheckPeriod) {
		this.processorCheckPeriod = processorCheckPeriod;
	}

	public long getReplicaIdleCheckPeriod() {
		return replicaIdleCheckPeriod;
	}

	public void setReplicaIdleCheckPeriod(long replicaIdleCheckPeriod) {
		this.replicaIdleCheckPeriod = replicaIdleCheckPeriod;
	}

	public long getReplicaHeartbeatPeriod() {
		return replicaHeartbeatPeriod;
	}

	public void setReplicaHeartbeatPeriod(long replicaHeartbeatPeriod) {
		this.replicaHeartbeatPeriod = replicaHeartbeatPeriod;
	}

	public long getMinSwitchtimeInterval() {
		return minSwitchtimeInterval;
	}

	public void setMinSwitchtimeInterval(long minSwitchtimeInterval) {
		this.minSwitchtimeInterval = minSwitchtimeInterval;
	}

	public boolean isLoadBalanceEnable() {
		return loadBalanceEnable;
	}

	public void setLoadBalanceEnable(boolean loadBalanceEnable) {
		this.loadBalanceEnable = loadBalanceEnable;
	}

	public String getLoadBalanceIp() {
		return loadBalanceIp;
	}

	public void setLoadBalanceIp(String loadBalanceIp) {
		this.loadBalanceIp = loadBalanceIp;
	}

	public int getLoadBalancePort() {
		return loadBalancePort;
	}

	public void setLoadBalancePort(int loadBalancePort) {
		this.loadBalancePort = loadBalancePort;
	}

	public int getPrepareDelaySeconds() {
		return prepareDelaySeconds;
	}

	public void setPrepareDelaySeconds(int prepareDelaySeconds) {
		this.prepareDelaySeconds = prepareDelaySeconds;
	}
}

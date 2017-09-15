package io.mycat.proxy;

import java.util.HashMap;
import java.util.Map;

/**
 * 代理的配置信息
 *
 * @author wuzhihui
 *
 */
public class ProxyConfig {
	// 绑定的数据传输IP地址
	private String bindIP = "0.0.0.0";
	// 绑定的数据传输端口
	private int bindPort = 8066;
	private boolean clusterEnable = false;
	// 集群通信绑定的IP地址
	private String clusterIP = "0.0.0.0";
	// 集群通信绑定的端口
	private int clusterPort = 9066;
	private String myNodeId;
	// 逗号分隔的所有集群节点的ID:IP:Port信息，如
	// leader-1:127.0.0.1:9066,leader-2:127.0.0.1:9068,leader-3:127.0.0.1:9069
	private String allNodeInfs;
	// 当前节点所用的配置文件的版本
	private Map<Byte, Integer> configVersionMap = new HashMap<>();
	private Map<Byte, Object> configMap = new HashMap<>();

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

	public Map<Byte, Integer> getConfigVersionMap() {
		return configVersionMap;
	}

	public int getConfigVersion(byte configKey) {
		Integer oldVersion = configVersionMap.get(configKey);
		return oldVersion == null ? ConfigEnum.INIT_VERSION : oldVersion;
	}

	public int getNextConfigVersion(byte configKey) {
		return getConfigVersion(configKey) + 1;
	}

	public Object getConfig(byte configKey) {
		return configMap.get(configKey);
	}

	public void putConfig(byte configKey, Object config, Integer version) {
		configMap.put(configKey, config);
		version = version == null ? ConfigEnum.INIT_VERSION : version;
		configVersionMap.put(configKey, version);
	}
}

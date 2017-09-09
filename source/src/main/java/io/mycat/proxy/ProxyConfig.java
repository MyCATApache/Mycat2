package io.mycat.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.mycat.mycat2.MycatConfig;

/**
 * 代理的配置信息
 *
 * @author wuzhihui
 *
 */
public class ProxyConfig {
	// 绑定的数据传输IP地址
	private String bindIP;
	// 绑定的数据传输端口
	private int bindPort;
	// 集群通信绑定的IP地址
	private String clusterIP = "0.0.0.0";
	// 集群通信绑定的端口
	private int clusterPort = 9066;
	private boolean clusterEnable = false;
	private String myNodeId;
	// 逗号分隔的所有集群节点的ID:IP:Port信息，如
	// leader-1:127.0.0.1:9066,leader-2:127.0.0.1:9068,leader-3:127.0.0.1:9069
	private String allNodeInfs;
	// 当前节点所用的配置文件的版本
	private Map<String, Integer> configVersionMap = new HashMap<>();

	public static MycatConfig loadFromProperties(InputStream in) throws IOException {
		try
		{
			MycatConfig conf = new MycatConfig();
			Properties pros = new Properties();
			pros.load(in);
			conf.setBindIP(pros.getProperty("bindip", "0.0.0.0"));
			conf.setBindPort(Integer.valueOf(pros.getProperty("bindport", "8066")));
			conf.setClusterIP(pros.getProperty("cluster.ip"));
			conf.setClusterPort(Integer.valueOf(pros.getProperty("cluster.port", "9066")));
			conf.setClusterEnable(Boolean.valueOf(pros.getProperty("cluster.enabled", "false")));
			conf.setMyNodeId(pros.getProperty("cluster.myid"));
			conf.setAllNodeInfs(pros.getProperty("cluster.allnodes"));
			//加载mycat.conf配置完毕设置版本号
			conf.putConfigVersion(ConfigKey.MYCAT_CONF, ConfigKey.INIT_VERSION);
			return conf;
		} finally {
			if (in!=null)
			{
				in.close();
			}
		}
	}

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

	public Map<String, Integer> getConfigVersionMap() {
		return configVersionMap;
	}

	public void putConfigVersion(String configKey, Integer configValue) {
		configVersionMap.put(configKey, configValue);
	}

	public int getConfigVersion(String configKey) {
		return configVersionMap.get(configKey);
	}

	public int configVersionGetAndIncrease(String configKey) {
		int oldVersion = configVersionMap.get(configKey);
		configVersionMap.put(configKey, oldVersion + 1);
		return oldVersion;
	}
}

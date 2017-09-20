package io.mycat.proxy;

import io.mycat.mycat2.beans.BalancerBean;
import io.mycat.mycat2.beans.ClusterBean;
import io.mycat.mycat2.beans.HeartbeatBean;
import io.mycat.mycat2.beans.ProxyBean;

/**
 * 代理的配置信息
 *
 * @author wuzhihui
 *
 */
public class ProxyConfig implements Configurable {
	/**
	 * 代理相关配置
	 */
	private ProxyBean proxy;
	/**
	 * 集群相关配置
	 */
	private ClusterBean cluster;
	/**
	 * 负载均衡相关配置
	 */
	private BalancerBean balancer;
	/**
	 * 心跳相关配置
	 */
	private HeartbeatBean heartbeat;

	public ProxyBean getProxy() {
		return proxy;
	}

	public void setProxy(ProxyBean proxy) {
		this.proxy = proxy;
	}

	public ClusterBean getCluster() {
		return cluster;
	}

	public void setCluster(ClusterBean cluster) {
		this.cluster = cluster;
	}

	public BalancerBean getBalancer() {
		return balancer;
	}

	public void setBalancer(BalancerBean balancer) {
		this.balancer = balancer;
	}

	public HeartbeatBean getHeartbeat() {
		return heartbeat;
	}

	public void setHeartbeat(HeartbeatBean heartbeat) {
		this.heartbeat = heartbeat;
	}
}

package io.mycat.proxy;

/**
 * 代理的配置信息
 * 
 * @author wuzhihui
 *
 */
public class ProxyConfig {
	private String bindIP;
	private int bindPort;
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
	
}

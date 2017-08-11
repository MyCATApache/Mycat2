package io.mycat.proxy;

/**
 * 代理的配置信息
 * 
 * @author wuzhihui
 *
 */
public class ProxyConfig {
	//绑定的数据传输IP地址
	private String bindIP;
	//绑定的数据传输端口
	private int bindPort;
	//是否开启管理端口
	private boolean adminPortEnable;
	//管理绑定的IP地址
	private String adminIP="0.0.0.0";
	//管理绑定的端口
	private int adminPort=9066;;
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
	public boolean isAdminPortEnable() {
		return adminPortEnable;
	}
	public void setAdminPortEnable(boolean adminPortEnable) {
		this.adminPortEnable = adminPortEnable;
	}
	public String getAdminIP() {
		return adminIP;
	}
	public void setAdminIP(String adminIP) {
		this.adminIP = adminIP;
	}
	public int getAdminPort() {
		return adminPort;
	}
	public void setAdminPort(int adminPort) {
		this.adminPort = adminPort;
	}
	
	
}

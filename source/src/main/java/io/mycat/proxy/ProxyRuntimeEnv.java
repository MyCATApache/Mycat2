package io.mycat.proxy;
/**
 * 运行时的一些信息，单例模式，供全局使用
 * @author wuzhihui
 *
 */
public class ProxyRuntimeEnv {

	public static final ProxyRuntimeEnv INSTANCE=new ProxyRuntimeEnv();
	
	private String bindIP;
	private int bindPort;
	private int nioReactorThreads=2;
	private ProxyReactorThread[] reactorThreads;
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
	public int getNioReactorThreads() {
		return nioReactorThreads;
	}
	public void setNioReactorThreads(int nioReactorThreads) {
		this.nioReactorThreads = nioReactorThreads;
	}
	public ProxyReactorThread[] getReactorThreads() {
		return reactorThreads;
	}
	public void setReactorThreads(ProxyReactorThread[] reactorThreads) {
		this.reactorThreads = reactorThreads;
	}
	
	
}

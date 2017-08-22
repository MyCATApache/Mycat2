package io.mycat.proxy;

import java.io.IOException;

import io.mycat.proxy.man.AdminCommandResovler;
import io.mycat.proxy.man.DefaultAdminSessionManager;

public class ProxyStarter {

	public static void main(String[] args) throws IOException {
		ProxyConfig config = new ProxyConfig();
		config.setBindIP("0.0.0.0");
		config.setBindPort(8080);
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		runtime.setProxyConfig(config);
		// runtime.setNioProxyHandler(new DefaultMySQLProxyHandler());
		// runtime.setNioProxyHandler(new DefaultDirectProxyHandler());
		int cpus = Runtime.getRuntime().availableProcessors();
		runtime.setNioReactorThreads(cpus);
		runtime.setReactorThreads(new ProxyReactorThread[cpus]);
		// runtime.setSessionManager(new DefaultTCPProxySessionManager());
		// Debug观察MySQL协议用
		//runtime.setSessionManager(new MySQLStudySessionManager());
		// Mycat 2.0 Session Manager
		// runtime.setSessionManager(new MycatSessionManager());
		runtime.init();
		ProxyReactorThread<?>[] nioThreads = runtime.getReactorThreads();
		for (int i = 0; i < cpus; i++) {
			ProxyReactorThread<?> thread = new ProxyReactorThread<>(new BufferPool(1024 * 10));
			thread.setName("NIO_Thread " + (i + 1));
			thread.start();
			nioThreads[i] = thread;
		}
		// 启动NIO Acceptor
		new NIOAcceptor(new BufferPool(1024 * 10)).start();
		if (config.isClusterEnable()) {
			runtime.setAdminSessionManager(new DefaultAdminSessionManager());
			runtime.setAdminCmdResolver(new AdminCommandResovler());

		}
		System.out.println(
				"*** Mycat NIO Proxy Server  *** ,NIO Threads " + nioThreads.length + " listen on " + config.getBindIP()
						+ ":" + config.getBindPort() + ",Cluster mode enabled " + config.isClusterEnable());

	}

}

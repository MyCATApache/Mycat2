package io.mycat.proxy;

import java.io.IOException;

import io.mycat.mycat2.DefaultMySQLProxyHandler;

public class ProxyStarter {

	public static void main(String[] args) throws IOException {
		
		ProxyRuntime runtime=ProxyRuntime.INSTANCE;
		runtime.setBindIP("0.0.0.0");
		runtime.setBindPort(8080);
		//runtime.setNioProxyHandler(new DefaultMySQLProxyHandler());
		//runtime.setNioProxyHandler(new DefaultDirectProxyHandler());
		int cpus= Runtime.getRuntime().availableProcessors();
		runtime.setNioReactorThreads(cpus);
		runtime.setReactorThreads(new ProxyReactorThread[cpus]);
		runtime.init();
		ProxyReactorThread[] nioThreads=runtime.getReactorThreads();
		for(int i=0;i<cpus;i++)
		{
			ProxyReactorThread thread=new ProxyReactorThread();
			thread.setName("NIO_Thread "+(i+1));
			thread.start();
			nioThreads[i]=thread;
		}
		//启动NIO Acceptor
		new NIOAcceptor().start();

	}

}

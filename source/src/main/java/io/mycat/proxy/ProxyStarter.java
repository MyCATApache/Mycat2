package io.mycat.proxy;

import java.io.IOException;

public class ProxyStarter {

	public static void main(String[] args) throws IOException {
		int cpus= Runtime.getRuntime().availableProcessors();
		ProxyRuntime runtime=ProxyRuntime.INSTANCE;
		runtime.setBindIP("0.0.0.0");
		runtime.setBindPort(8080);
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

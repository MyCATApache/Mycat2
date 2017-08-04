package io.mycat.proxy;

import java.io.IOException;

public class ProxyStarter {

	public static void main(String[] args) throws IOException {
		int cpus= Runtime.getRuntime().availableProcessors();
		ProxyRuntimeEnv env=ProxyRuntimeEnv.INSTANCE;
		env.setBindIP("0.0.0.0");
		env.setBindPort(8080);
		env.setNioReactorThreads(cpus);
		env.setReactorThreads(new ProxyReactorThread[cpus]);
		ProxyReactorThread[] nioThreads=env.getReactorThreads();
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

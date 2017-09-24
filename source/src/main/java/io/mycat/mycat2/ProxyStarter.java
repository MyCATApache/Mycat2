package io.mycat.mycat2;

import java.io.IOException;

import io.mycat.mycat2.beans.ReplicaConfBean;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.mycat2.beans.SchemaConfBean;
import io.mycat.mycat2.loadbalance.LocalLoadChecker;
import io.mycat.mycat2.loadbalance.RandomStrategy;
import io.mycat.proxy.*;
import io.mycat.proxy.NIOAcceptor.ServerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.BufferPool;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOAcceptor;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.AdminCommandResovler;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.MyCluster;

public class ProxyStarter {
	private static final Logger LOGGER = LoggerFactory.getLogger(ProxyStarter.class);

	public static final ProxyStarter INSTANCE = new ProxyStarter();

	private ProxyStarter(){}

	public void start() throws IOException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		MycatConfig conf = (MycatConfig) runtime.getProxyConfig();

		// 启动NIO Acceptor
		NIOAcceptor acceptor = new NIOAcceptor(new BufferPool(1024 * 10));
		acceptor.start();
		runtime.setAcceptor(acceptor);

		if (conf.isClusterEnable()) {
			// 集群开启状态，需要等集群启动，主节点确认完配置才能提供服务
			acceptor.startServerChannel(conf.getClusterIP(), conf.getClusterPort(), ServerType.CLUSTER);
			runtime.setAdminCmdResolver(new AdminCommandResovler());
			MyCluster cluster = new MyCluster(acceptor.getSelector(), conf.getMyNodeId(), ClusterNode.parseNodesInf(conf.getAllNodeInfs()));
			runtime.setMyCLuster(cluster);
			cluster.initCluster();
		} else {
			// 未配置集群，直接启动
			startProxy(true);
		}
	}

	public void startProxy(boolean isLeader) throws IOException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		MycatConfig conf = (MycatConfig) runtime.getProxyConfig();

		// 加载配置文件信息
		ConfigLoader.INSTANCE.loadAll();
		NIOAcceptor acceptor = runtime.getAcceptor();
		acceptor.startServerChannel(conf.getBindIP(), conf.getBindPort(), ServerType.MYCAT);
		startReactor();
		// 初始化
		init(conf);
        if(conf.isLoadBalanceEnable()){
            //开启负载均衡服务
            runtime.setLocalLoadChecker(new LocalLoadChecker());
            runtime.setLoadBalanceStrategy(new RandomStrategy());
            acceptor.startServerChannel(conf.getLoadBalanceIp(), conf.getLoadBalancePort(), ServerType.LOAD_BALANCER);
        }

		// 主节点才启动心跳，非集群下也启动心跳
		if (isLeader) {
			runtime.startHeartBeatScheduler();
		}
	}

	public void stopProxy() {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		NIOAcceptor acceptor = runtime.getAcceptor();
		acceptor.stopServerChannel(false);

		runtime.stopHeartBeatScheduler();
	}

	private void startReactor() throws IOException {
		// Mycat 2.0 Session Manager
		MycatReactorThread[] nioThreads = (MycatReactorThread[]) MycatRuntime.INSTANCE.getReactorThreads();
		int cpus = nioThreads.length;
		for (int i = 0; i < cpus; i++) {
			MycatReactorThread thread = new MycatReactorThread(new BufferPool(1024 * 10));
			thread.setName("NIO_Thread " + (i + 1));
			thread.start();
			nioThreads[i] = thread;
		}
	}

	private void init(MycatConfig conf) {
		// 初始化连接
		conf.getMysqlRepMap().forEach((key, value) -> {
			value.initMaster();
			value.getMysqls().forEach(metaBean -> {
				try {
					metaBean.init(value,ProxyRuntime.INSTANCE.maxdataSourceInitTime,value.getDataSourceInitStatus());
				} catch (IOException e) {
					LOGGER.error("error to init metaBean: {}", metaBean.getHostName());
				}
			});
		});
	}
}

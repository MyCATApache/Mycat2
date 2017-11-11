package io.mycat.mycat2;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.beans.conf.BalancerBean;
import io.mycat.mycat2.beans.conf.BalancerConfig;
import io.mycat.mycat2.beans.conf.ClusterBean;
import io.mycat.mycat2.beans.conf.ClusterConfig;
import io.mycat.mycat2.beans.conf.ProxyBean;
import io.mycat.mycat2.beans.conf.ProxyConfig;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOAcceptor;
import io.mycat.proxy.NIOAcceptor.ServerType;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.buffer.DirectByteBufferPool;
import io.mycat.proxy.man.AdminCommandResovler;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.MyCluster;

public class ProxyStarter {
	private static final Logger LOGGER = LoggerFactory.getLogger(ProxyStarter.class);
	public static final ProxyStarter INSTANCE = new ProxyStarter();

	/**
	 * 用于初始化启动
	 * @throws IOException
	 */
	public void start() throws IOException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		MycatConfig conf = runtime.getConfig();
		ProxyConfig proxyConfig = conf.getConfig(ConfigEnum.PROXY);
		ProxyBean proxybean = proxyConfig.getProxy();
		// 启动NIO Acceptor
		NIOAcceptor acceptor = new NIOAcceptor(new DirectByteBufferPool(proxybean.getBufferPoolPageSize(),
				proxybean.getBufferPoolChunkSize(),
				proxybean.getBufferPoolPageNumber()));
		acceptor.start();
		runtime.setAcceptor(acceptor);

		ClusterConfig clusterConfig = conf.getConfig(ConfigEnum.CLUSTER);
		ClusterBean clusterBean = clusterConfig.getCluster();
		if (clusterBean.isEnable()) {
			// 启动集群
			startCluster(runtime, clusterBean, acceptor);
		} else {
			// 未配置集群，直接启动
			startProxy(true);
		}
	}

	/**
	 * 集群模式下启动先启动admin对应的端口，等集群建立成功后才加载配置启动proxy
	 * @param runtime
	 * @param clusterBean
	 * @param acceptor
	 * @throws IOException
	 */
	private void startCluster(ProxyRuntime runtime, ClusterBean clusterBean, NIOAcceptor acceptor) throws IOException {
		// 集群模式下，需要等集群启动，主节点确认完配置才能提供服务
		acceptor.startServerChannel(clusterBean.getIp(), clusterBean.getPort(), ServerType.CLUSTER);

		MyCluster cluster = new MyCluster(acceptor.getSelector(), clusterBean.getMyNodeId(), ClusterNode.parseNodesInf(clusterBean.getAllNodes()));
		runtime.setAdminCmdResolver(new AdminCommandResovler());
		runtime.setMyCLuster(cluster);
		cluster.initCluster();
	}

	/**
	 * 启动代理
	 * @param isLeader true 主节点，false 从节点
	 * @throws IOException
	 */
	public void startProxy(boolean isLeader) throws IOException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		MycatConfig conf = runtime.getConfig();
		NIOAcceptor acceptor = runtime.getAcceptor();

		ProxyConfig proxyConfig = conf.getConfig(ConfigEnum.PROXY);
		ProxyBean proxyBean = proxyConfig.getProxy();
		if (acceptor.startServerChannel(proxyBean.getIp(), proxyBean.getPort(), ServerType.MYCAT)){
			startReactor();

			// 加载配置文件信息
			ConfigLoader.INSTANCE.loadAll();

			ProxyRuntime.INSTANCE.getConfig().initRepMap();
			ProxyRuntime.INSTANCE.getConfig().initSchemaMap();

			conf.getMysqlRepMap().forEach((repName, repBean) -> {
				repBean.initMaster();
				repBean.getMetaBeans().forEach(metaBean -> metaBean.prepareHeartBeat(repBean, repBean.getDataSourceInitStatus()));
			});
		}

		ClusterConfig clusterConfig = conf.getConfig(ConfigEnum.CLUSTER);
		ClusterBean clusterBean = clusterConfig.getCluster();
		// 主节点才启动心跳，非集群按主节点处理
		if (isLeader) {
			runtime.startHeartBeatScheduler();
		}

		BalancerConfig balancerConfig = conf.getConfig(ConfigEnum.BALANCER);
		BalancerBean balancerBean = balancerConfig.getBalancer();
		// 集群模式下才开启负载均衡服务
        if (clusterBean.isEnable() && balancerBean.isEnable()) {
			runtime.getAcceptor().startServerChannel(balancerBean.getIp(), balancerBean.getPort(), ServerType.LOAD_BALANCER);
        }
	}

	public void stopProxy() {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		NIOAcceptor acceptor = runtime.getAcceptor();
		acceptor.stopServerChannel(false);
		//todo 关闭所有前后端连接？

		runtime.stopHeartBeatScheduler();
	}

	private void startReactor() throws IOException {
		// Mycat 2.0 Session Manager
		MycatReactorThread[] nioThreads = (MycatReactorThread[]) MycatRuntime.INSTANCE.getReactorThreads();
		ProxyConfig proxyConfig = ProxyRuntime.INSTANCE.getConfig().getConfig(ConfigEnum.PROXY);
		ProxyBean proxybean = proxyConfig.getProxy();
		int cpus = nioThreads.length;
		for (int i = 0; i < cpus; i++) {
			MycatReactorThread thread = new MycatReactorThread(
					new DirectByteBufferPool(proxybean.getBufferPoolPageSize(),
							proxybean.getBufferPoolChunkSize(),
							proxybean.getBufferPoolPageNumber()));
			thread.setName("NIO_Thread " + (i + 1));
			thread.start();
			nioThreads[i] = thread;
		}
	}
}

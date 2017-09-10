package io.mycat.mycat2;

import java.io.IOException;

import io.mycat.mycat2.beans.ReplicaConfBean;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.mycat2.beans.SchemaConfBean;
import io.mycat.proxy.*;
import io.mycat.proxy.man.AdminCommandResovler;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.MyCluster;
import io.mycat.util.YamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			acceptor.startServerChannel(conf.getClusterIP(), conf.getClusterPort(), true);
			runtime.setAdminCmdResolver(new AdminCommandResovler());
			MyCluster cluster = new MyCluster(acceptor.getSelector(), conf.getMyNodeId(), ClusterNode.parseNodesInf(conf.getAllNodeInfs()));
			runtime.setMyCLuster(cluster);
			cluster.initCluster();
		} else {
			// 未配置集群，直接启动
			startProxy(true);
		}
	}

	public void startProxy(boolean isMaster) throws IOException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		MycatConfig conf = (MycatConfig) runtime.getProxyConfig();
//		if (isMaster) {
			// 开启mycat服务
		loadConfig(conf);
		NIOAcceptor acceptor = runtime.getAcceptor();
		acceptor.startServerChannel(conf.getBindIP(), conf.getBindPort(), false);
		startReactor();
		// 初始化
		init(conf);
//		}
	}

	public void stopProxy() {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		NIOAcceptor acceptor = runtime.getAcceptor();
		acceptor.stopServerChannel(false);
	}

	private void startReactor() throws IOException {
		// Mycat 2.0 Session Manager
		ProxyReactorThread<?>[] nioThreads = ProxyRuntime.INSTANCE.getReactorThreads();
		int cpus = nioThreads.length;
		for (int i = 0; i < cpus; i++) {
			ProxyReactorThread<?> thread = new ProxyReactorThread<>(new BufferPool(1024 * 10));
			thread.setName("NIO_Thread " + (i + 1));
			thread.start();
			nioThreads[i] = thread;
		}
	}

	private void loadConfig(MycatConfig conf) throws IOException {
		// 加载replica-index
		LOGGER.debug("load config for {}", "replica-index.yml");
		ReplicaIndexBean replicaIndexBean = YamlUtil.load("replica-index.yml", ReplicaIndexBean.class);
		conf.addRepIndex(replicaIndexBean);
		conf.putConfigVersion(ConfigKey.REPLICA_INDEX, ConfigKey.INIT_VERSION);

		// 加载datasource
		LOGGER.debug("load config for {}", "datasource.yml");
		ReplicaConfBean replicaConfBean = YamlUtil.load("datasource.yml", ReplicaConfBean.class);
		replicaConfBean.getMysqlReplicas().forEach(replicaBean -> {
			conf.addMySQLRepBean(replicaBean);
		});
		conf.putConfigVersion(ConfigKey.DATASOURCE, ConfigKey.INIT_VERSION);

		// 加载schema
		LOGGER.debug("load config for {}", "schema.yml");
		SchemaConfBean schemaConfBean = YamlUtil.load("schema.yml", SchemaConfBean.class);
		schemaConfBean.getSchemas().forEach(schemaBean -> {
			conf.addSchemaBean(schemaBean);
		});
		conf.putConfigVersion(ConfigKey.SCHEMA, ConfigKey.INIT_VERSION);
	}

	private void init(MycatConfig conf) {
		// 初始化连接
		conf.getMysqlRepMap().forEach((key, value) -> {
			value.initMaster();
			value.getMysqls().forEach(metaBean -> {
				try {
					metaBean.init();
				} catch (IOException e) {
					LOGGER.error("error to init metaBean: {}", metaBean.getHostName());
				}
			});
		});
	}
}

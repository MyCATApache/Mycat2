package io.mycat.mycat2;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.proxy.*;
import io.mycat.proxy.man.AdminCommandResovler;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.DefaultAdminSessionManager;
import io.mycat.proxy.man.MyCluster;
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
		runtime.setAcceptor(acceptor);

		if (conf.isClusterEnable()) {
			// 集群开启状态，需要等集群启动，主节点确认完配置才能提供服务
			acceptor.startServerChannel(conf.getClusterIP(), conf.getClusterPort(), true);
			startProxyReactorThread();

			runtime.setAdminCmdResolver(new AdminCommandResovler());
			MyCluster cluster = new MyCluster(acceptor.getSelector(), conf.getMyNodeId(), ClusterNode.parseNodesInf(conf.getAllNodeInfs()));
			runtime.setMyCLuster(cluster);
			cluster.initCluster();
		} else {
			// 未配置集群，直接启动
			startProxy(true);
		}
		acceptor.start();
	}

	public void startProxy(boolean isMaster) throws IOException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		MycatConfig conf = (MycatConfig) runtime.getProxyConfig();
//		if (isMaster) {
			// 开启mycat服务
			NIOAcceptor acceptor = runtime.getAcceptor();
			acceptor.startServerChannel(conf.getBindIP(), conf.getBindPort(), false);
			startProxyReactorThread();
			loadConfig();
//		}
	}

	public void stopProxy() {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		NIOAcceptor acceptor = runtime.getAcceptor();
		acceptor.stopServerChannel(false);
	}

	private void startProxyReactorThread() throws IOException {
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

	private void loadConfig() throws IOException {
		MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();

		// 加载replica-index
		loadProperties(conf, "replica-index.properties");

		// 加载datasource.xml
		URL datasourceURL = ConfigLoader.class.getResource("/datasource.xml");
		List<MySQLRepBean> mysqlRepBeans = ConfigLoader.loadMySQLRepBean(datasourceURL.toString());
		mysqlRepBeans.forEach(mySQLRepBean -> mySQLRepBean.getMysqls().forEach(mySQLMetaBean -> {
			try {
				LOGGER.debug("begin to init mySQLMetaBean: {}", mySQLMetaBean);
				mySQLMetaBean.init();
			} catch (IOException e) {
				LOGGER.error("error to init mySQLMetaBean: {}", mySQLMetaBean);
			}
		}));
		conf.addMySQLRepBeanList(mysqlRepBeans);
		conf.putConfigVersion(ConfigKey.DATASOURCE, ConfigKey.INIT_VERSION);

		// 加载schema.xml
		URL schemaURL = ConfigLoader.class.getResource("/schema.xml");
		List<SchemaBean> schemaBeans = ConfigLoader.loadSheamBeans(schemaURL.toString());
		conf.addSchemaBeanList(schemaBeans);
		conf.putConfigVersion(ConfigKey.SCHEMA, ConfigKey.INIT_VERSION);
	}

	private void loadProperties(MycatConfig conf, String propName) throws IOException {
		System.out.println("look Java Classpath for " + propName);
		InputStream instream = ClassLoader.getSystemResourceAsStream(propName);
		instream = (instream == null) ? ConfigLoader.class.getResourceAsStream("/"+propName) : instream;
		try
		{
			Properties props = new Properties();
			props.load(instream);
			props.forEach((key, value) -> conf.addRepIndex((String)key, Integer.parseInt((String)value)));
			//加载完毕设置配置文件版本，默认为1
			conf.putConfigVersion(ConfigKey.REPLICA_INDEX, ConfigKey.INIT_VERSION);
		} finally {
			if(instream!=null) {
				instream.close();
			}
		}
	}
}

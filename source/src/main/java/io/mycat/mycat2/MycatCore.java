/*
 * Copyright (c) 2016, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://mycat.io/
 *
 */
package io.mycat.mycat2;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.mycat2.common.ExecutorUtil;
import io.mycat.mycat2.common.NameableExecutor;
import io.mycat.mycat2.common.NamebleScheduledExecutor;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.NIOAcceptor;
import io.mycat.proxy.ProxyReactorThread;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.AdminCommandResovler;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.DefaultAdminSessionHandler;
import io.mycat.proxy.man.DefaultAdminSessionManager;
import io.mycat.proxy.man.MyCluster;

/**
 * @author wuzhihui
 */
public class MycatCore {
	private static final Logger logger = LoggerFactory.getLogger(MycatCore.class);
	public static final String MOCK_HOSTNAME = "host1";

	public static final String MOCK_SCHEMA = "mysql";

	public static void main(String[] args) throws IOException {
		// Business Executor ，用来执行那些耗时的任务
		NameableExecutor businessExecutor = ExecutorUtil.create("BusinessExecutor", 10);
		// 定时器Executor，用来执行定时任务
		NamebleScheduledExecutor timerExecutor = ExecutorUtil.createSheduledExecute("Timer", 5);
		InputStream instream=ClassLoader.getSystemResourceAsStream("mycat.conf");
		 instream=(instream==null)?ConfigLoader.class.getResourceAsStream("/mycat.conf"):instream;
		MycatConfig conf = MycatConfig.loadFromProperties(instream);
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		runtime.setProxyConfig(conf);
		// runtime.setNioProxyHandler(new DefaultMySQLProxyHandler());
		// runtime.setNioProxyHandler(new DefaultDirectProxyHandler());
		int cpus = Runtime.getRuntime().availableProcessors();
		runtime.setNioReactorThreads(cpus);
		runtime.setReactorThreads(new ProxyReactorThread[cpus]);
		// runtime.setSessionManager(new DefaultTCPProxySessionManager());
		// Debug观察MySQL协议用
		// runtime.setSessionManager(new MySQLStudySessionManager());
		// Mycat 2.0 Session Manager
		runtime.setSessionManager(new MycatSessionManager());
		runtime.init();
		ProxyReactorThread<?>[] nioThreads = runtime.getReactorThreads();
		for (int i = 0; i < cpus; i++) {
			ProxyReactorThread<?> thread = new ProxyReactorThread<>(new BufferPool(1024 * 10));
			thread.setName("NIO_Thread " + (i + 1));
			thread.start();
			nioThreads[i] = thread;
		}
		// 启动NIO Acceptor
		NIOAcceptor acceptor = new NIOAcceptor(new BufferPool(1024 * 10));
		acceptor.start();
		if (conf.isClusterEnable()) {
			runtime.setAdminSessionManager(new DefaultAdminSessionManager());
			ClusterNode myNode = new ClusterNode(conf.getMyNodeId(), conf.getClusterIP(), conf.getClusterPort());
			MyCluster cluster = new MyCluster(acceptor.getSelector(), myNode,
					ClusterNode.parseNodesInf(conf.getAllNodeInfs()));
			runtime.setMyCLuster(cluster);
			cluster.initCluster();

		}

		URL datasourceURL = ConfigLoader.class.getResource("/datasource.xml");
		List<MySQLRepBean> mysqlRepBeans = ConfigLoader.loadMySQLRepBean(datasourceURL.toString());
		for (final MySQLRepBean repBean : mysqlRepBeans) {
			MySQLReplicatSet mysqlRepSet = new MySQLReplicatSet(repBean, 0);
			conf.addMySQLReplicatSet(mysqlRepSet);
		}
		URL schemaURL = ConfigLoader.class.getResource("/schema.xml");
		List<SchemaBean> schemaBeans = ConfigLoader.loadSheamBeans(schemaURL.toString());
		for (SchemaBean schemaBean : schemaBeans) {
			conf.addSchemaBean(schemaBean);
		}
	}
}

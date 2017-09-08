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

import io.mycat.proxy.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.common.ExecutorUtil;
import io.mycat.mycat2.common.NameableExecutor;
import io.mycat.mycat2.common.NamebleScheduledExecutor;

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

		InputStream instream = null;
		String mySeq="1";
		if (args.length > 0) {
			mySeq=args[0];
		}
		String mycatConf="mycat"+mySeq+".conf";
		System.out.println("look Java Classpath for Mycat config file "+mycatConf);
		if (instream == null) {
			instream = ClassLoader.getSystemResourceAsStream(mycatConf);
		}
		instream = (instream == null) ? ConfigLoader.class.getResourceAsStream("/"+mycatConf) : instream;
		// mycat.conf的加载不需要在集群内
		MycatConfig conf = MycatConfig.loadFromProperties(instream);

		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		runtime.setProxyConfig(conf);

		int cpus = Runtime.getRuntime().availableProcessors();
		runtime.setNioReactorThreads(cpus);
		runtime.setReactorThreads(new ProxyReactorThread[cpus]);

		// runtime.setNioProxyHandler(new DefaultMySQLProxyHandler());
		// runtime.setNioProxyHandler(new DefaultDirectProxyHandler());
		// runtime.setSessionManager(new DefaultTCPProxySessionManager());
		// Debug观察MySQL协议用
		// runtime.setSessionManager(new MySQLStudySessionManager());
		runtime.setSessionManager(new MycatSessionManager());
		runtime.init();

		ProxyStarter.INSTANCE.start();
	}
}

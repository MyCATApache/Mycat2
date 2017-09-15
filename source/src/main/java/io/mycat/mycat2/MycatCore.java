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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.YamlUtil;

/**
 * @author wuzhihui
 */
public class MycatCore {
	private static final Logger logger = LoggerFactory.getLogger(MycatCore.class);
	public static final String MOCK_HOSTNAME = "host1";

	public static final String MOCK_SCHEMA = "mysql";

	public static void main(String[] args) throws IOException {

		String mySeq = "1";
		if (args.length > 0) {
			mySeq = args[0];
		}
		String mycatConf = "mycat" + mySeq + ".yml";
		logger.debug("load config for {}", mycatConf);
		// mycat.conf的加载不需要在集群内
		MycatConfig conf = YamlUtil.load(mycatConf, MycatConfig.class);

		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		runtime.setProxyConfig(conf);

		int cpus = Runtime.getRuntime().availableProcessors();
		runtime.setNioReactorThreads(cpus);
		runtime.setReactorThreads(new MycatReactorThread[cpus]);

		// runtime.setNioProxyHandler(new DefaultMySQLProxyHandler());
		// runtime.setNioProxyHandler(new DefaultDirectProxyHandler());
		// runtime.setSessionManager(new DefaultTCPProxySessionManager());
		// Debug观察MySQL协议用
		// runtime.setSessionManager(new MySQLStudySessionManager());
		runtime.setSessionManager(new MycatSessionManager());

		ProxyStarter.INSTANCE.start();
		
		runtime.init();
	}
}

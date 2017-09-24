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

import io.mycat.mycat2.beans.conf.ProxyConfig;
import io.mycat.proxy.ConfigEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.YamlUtil;

/**
 * @author wuzhihui
 */
public class MycatCore {
	public static void main(String[] args) throws IOException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		MycatConfig conf = new MycatConfig();
		runtime.setConfig(conf);

		ConfigLoader.INSTANCE.loadCore();

		int cpus = Runtime.getRuntime().availableProcessors();
//	    int cpus = 1;
		runtime.setNioReactorThreads(cpus);
		runtime.setReactorThreads(new MycatReactorThread[cpus]);

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

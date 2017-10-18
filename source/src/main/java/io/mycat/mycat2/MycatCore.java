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

import io.mycat.mycat2.beans.ArgsBean;
import io.mycat.mycat2.beans.conf.BalancerBean;
import io.mycat.mycat2.beans.conf.BalancerConfig;
import io.mycat.mycat2.beans.conf.ClusterConfig;
import io.mycat.mycat2.beans.conf.ProxyConfig;
import io.mycat.proxy.ConfigEnum;

import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.ProxyRuntime;

/**
 * @author wuzhihui
 */
public class MycatCore {
	public static void main(String[] args) throws IOException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		runtime.setConfig(new MycatConfig());

		ConfigLoader.INSTANCE.loadCore();
		solveArgs(args);

		int cpus = Runtime.getRuntime().availableProcessors();
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

	private static void solveArgs(String[] args) {
		int lenght = args.length;

		MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
		ProxyConfig proxyConfig = conf.getConfig(ConfigEnum.PROXY);
		ClusterConfig clusterConfig = conf.getConfig(ConfigEnum.CLUSTER);
		BalancerConfig balancerConfig= conf.getConfig(ConfigEnum.BALANCER);

		for (int i = 0; i < lenght; i++) {
			switch(args[i]) {
				case ArgsBean.PROXY_PORT:
					proxyConfig.getProxy().setPort(Integer.parseInt(args[++i]));
					break;
				case ArgsBean.CLUSTER_ENABLE:
					clusterConfig.getCluster().setEnable(Boolean.parseBoolean(args[++i]));
					break;
				case ArgsBean.CLUSTER_PORT:
					clusterConfig.getCluster().setPort(Integer.parseInt(args[++i]));
					break;
				case ArgsBean.CLUSTER_MY_NODE_ID:
					clusterConfig.getCluster().setMyNodeId(args[++i]);
					break;
				case ArgsBean.BALANCER_ENABLE:
					balancerConfig.getBalancer().setEnable(Boolean.parseBoolean(args[++i]));
					break;
				case ArgsBean.BALANCER_PORT:
					balancerConfig.getBalancer().setPort(Integer.parseInt(args[++i]));
					break;
				case ArgsBean.BALANCER_STRATEGY:
					BalancerBean.BalancerStrategyEnum strategy = BalancerBean.BalancerStrategyEnum.getEnum(args[++i]);
					if (strategy == null) {
						throw new IllegalArgumentException("no such balancer strategy");
					}
					balancerConfig.getBalancer().setStrategy(strategy);
					break;
				default:
					break;
			}
		}
	}
}

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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger LOGGER = LoggerFactory.getLogger(MycatCore.class);

	public static void main(String[] args) throws IOException, ParseException {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		runtime.setConfig(new MycatConfig());

		ConfigLoader.INSTANCE.loadCore();
		solveArgs(args);

		int cpus = Runtime.getRuntime().availableProcessors();
		runtime.setMycatReactorThreads(new MycatReactorThread[cpus]);

		// runtime.setNioProxyHandler(new DefaultMySQLProxyHandler());
		// runtime.setNioProxyHandler(new DefaultDirectProxyHandler());
		// runtime.setSessionManager(new DefaultTCPProxySessionManager());
		// Debug观察MySQL协议用
		// runtime.setSessionManager(new MySQLStudySessionManager());
		runtime.setSessionManager(new MycatSessionManager());
		runtime.init();

		ProxyStarter.INSTANCE.start();
	}

	private static void solveArgs(String[] args) throws ParseException {
		Options options = new Options();
		options.addOption(null, ArgsBean.PROXY_PORT,true,"proxy port");
		options.addOption(null, ArgsBean.CLUSTER_ENABLE,true,"cluster enable");
		options.addOption(null, ArgsBean.CLUSTER_PORT,true,"cluster port");
		options.addOption(null, ArgsBean.CLUSTER_MY_NODE_ID,true,"cluster my node id");
		options.addOption(null, ArgsBean.BALANCER_ENABLE,true,"balancer enable");
		options.addOption(null, ArgsBean.BALANCER_PORT,true,"balancer port");
		options.addOption(null, ArgsBean.BALANCER_STRATEGY,true,"balancer strategy");

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options,args);

		MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
		ProxyConfig proxyConfig = conf.getConfig(ConfigEnum.PROXY);
		ClusterConfig clusterConfig = conf.getConfig(ConfigEnum.CLUSTER);
		BalancerConfig balancerConfig= conf.getConfig(ConfigEnum.BALANCER);

		if (cmd.hasOption(ArgsBean.PROXY_PORT)) {
			proxyConfig.getProxy().setPort(Integer.parseInt(cmd.getOptionValue(ArgsBean.PROXY_PORT)));
		}

		if (cmd.hasOption(ArgsBean.CLUSTER_ENABLE)){
			clusterConfig.getCluster().setEnable(Boolean.parseBoolean(cmd.getOptionValue(ArgsBean.CLUSTER_ENABLE)));
		}
		if (cmd.hasOption(ArgsBean.CLUSTER_PORT)){
			clusterConfig.getCluster().setPort(Integer.parseInt(cmd.getOptionValue(ArgsBean.CLUSTER_PORT)));
		}
		if (cmd.hasOption(ArgsBean.CLUSTER_MY_NODE_ID)){
			clusterConfig.getCluster().setMyNodeId(cmd.getOptionValue(ArgsBean.CLUSTER_MY_NODE_ID));
		}

		if (cmd.hasOption(ArgsBean.BALANCER_ENABLE)){
			balancerConfig.getBalancer().setEnable(Boolean.parseBoolean(cmd.getOptionValue(ArgsBean.BALANCER_ENABLE)));
		}
		if (cmd.hasOption(ArgsBean.BALANCER_PORT)){
			balancerConfig.getBalancer().setPort(Integer.parseInt(cmd.getOptionValue(ArgsBean.BALANCER_PORT)));
		}
		if (cmd.hasOption(ArgsBean.BALANCER_STRATEGY)){
			BalancerBean.BalancerStrategyEnum strategy = BalancerBean.BalancerStrategyEnum.getEnum(cmd.getOptionValue(ArgsBean.BALANCER_STRATEGY));
			if (strategy == null) {
				throw new IllegalArgumentException("no such balancer strategy");
			}
			balancerConfig.getBalancer().setStrategy(strategy);
		}
		// 防止配置错误，做提示
		if (ArrayUtils.isNotEmpty(cmd.getArgs())) {
			LOGGER.warn("please check if param is correct");
		}
	}
}

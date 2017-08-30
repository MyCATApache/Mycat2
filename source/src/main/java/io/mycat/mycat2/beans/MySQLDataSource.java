/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
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
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.mycat2.beans;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.tasks.BackendCharsetReadTask;
import io.mycat.mycat2.tasks.BackendConCreateTask;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.ProxyReactorThread;
import io.mycat.proxy.ProxyRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据节点（数据库）的连接池
 *
 * @author wuzhihui
 */
public class MySQLDataSource {
	public static final Logger LOGGER = LoggerFactory.getLogger(MySQLDataSource.class);
	private final String name;
	private final AtomicInteger activeSize;
	private final MySQLBean mysqlBean;
	private final ConMap conMap = new ConMap();
	private long heartbeatRecoveryTime;
	private boolean slaveNode;

	/** collationIndex 和 charsetName 的映射 */
	public final Map<Integer, String> INDEX_TO_CHARSET = new HashMap<>();
	/** charsetName 到 默认collationIndex 的映射 */
	public final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();

	private TransferQueue<MySQLSession> sessionQueue = new LinkedTransferQueue<>();

	public MySQLSession getSession() {
		MySQLSession session = sessionQueue.poll();
		if (session != null) {
			return session;
		}

		//todo 新建连接
		return null;
	}

	public MySQLDataSource(MySQLBean config, boolean islaveNode) {
		this.activeSize = new AtomicInteger(0);
		this.mysqlBean = config;
		this.name = config.getHostName();
		this.slaveNode = islaveNode;
	}

	public void createMySQLSession(BufferPool bufferPool, Selector selector) {
		try {
			BackendConCreateTask authProcessor = new BackendConCreateTask(bufferPool, selector, this, null);
			authProcessor.setCallback((optSession, sender, exeSucces, retVal) -> {
				if (exeSucces) {
					int curSize = activeSize.incrementAndGet();
					if (curSize == 1) {
						BackendCharsetReadTask backendCharsetReadTask = new BackendCharsetReadTask(optSession, this);
						optSession.setCurNIOHandler(backendCharsetReadTask);
						backendCharsetReadTask.readCharset();
					}
					sessionQueue.add(optSession);
				}
			});
		} catch (IOException e) {
			LOGGER.warn("error to create mysqlSession for datasource: {}", this.name);
		}
	}

	public boolean isSlaveNode() {
		return slaveNode;
	}

	public void setSlaveNode(boolean slaveNode) {
		this.slaveNode = slaveNode;
	}

	public AtomicInteger getActiveSize() {
		return activeSize;
	}

	public String getName() {
		return name;
	}

	public boolean initSource() {
		int initSize = this.mysqlBean.getMinCon();
		LOGGER.info("init backend myqsl source ,create connections total " + initSize + " for " + mysqlBean);

		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		ProxyReactorThread[] reactorThreads = runtime.getReactorThreads();
		int reactorSize = runtime.getNioReactorThreads();
		for (int i = 0; i < initSize; i++) {
			ProxyReactorThread reactorThread = reactorThreads[i % reactorSize];
			reactorThread.addNIOJob(() -> createMySQLSession(reactorThread.getBufPool(), reactorThread.getSelector()));
		}

		LOGGER.info("init source finished");
		return true;
	}

	public BackConnection getConnection(String schema) throws IOException {
		return this.conMap.tryTakeCon(schema);
		// if (con != null) {
		// takeCon(con, // handler,
		// mySQLFrontConnection, schema);
		// return con;
		// }
		// else
		// {
		// int activeCons = this.getActiveCount();// 当前最大活动连接
		// if (activeCons + 1 > size) {// 下一个连接大于最大连接数
		// LOGGER.error("the max activeConnnections size can not be max than
		// maxconnections");
		// throw new IOException("the max activeConnnections size can not be max
		// than maxconnections");
		// } else { // create connection
		// LOGGER.info(
		// "no ilde connection in pool,create new connection for " + this.name +
		// " of schema " + schema);
		// MySQLBackendConnection mySQLBackendConnection =
		// createNewConnectionOnReactor(reactor, schema,
		// mySQLFrontConnection, userCallback);
		// mySQLFrontConnection.setBackendConnection(mySQLBackendConnection);
		// return mySQLBackendConnection;
		// }
		// }

	}

	public void releaseChannel(BackConnection c) {
		// c.setLastTime(TimeUtil.currentTimeMillis());
		this.conMap.returnCon(c);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("release channel " + c);
		}
	}

	public void connectionClosed(BackConnection conn) {
		conMap.getSchemaConnections(conn.schema).remove(conn);

	}

	public long getHeartbeatRecoveryTime() {
		return heartbeatRecoveryTime;
	}

	public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
		this.heartbeatRecoveryTime = heartbeatRecoveryTime;
	}

	public MySQLBean getConfig() {
		return mysqlBean;
	}

	@Override
	public String toString() {
		final StringBuilder sbuf = new StringBuilder("MySQLDataSource[").append("name=").append(name).append(',')
				.append("activeSize=").append(activeSize).append(',').append("heartbeatRecoveryTime=").append(heartbeatRecoveryTime)
				.append(',').append("slaveNode=").append(slaveNode).append(',').append("mysqlBean=").append(mysqlBean)
				.append(']');
		return (sbuf.toString());
	}

}

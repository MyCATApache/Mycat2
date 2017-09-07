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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.net.DefaultMycatSessionHandler;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mycat2.tasks.BackendCharsetReadTask;
import io.mycat.mycat2.tasks.BackendConCreateTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.ProxyReactorThread;
import io.mycat.proxy.ProxyRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public MySQLSession getExistsSession() throws ClosedChannelException {
		MySQLSession session = sessionQueue.poll();
		ProxyReactorThread reactorThread = (ProxyReactorThread) Thread.currentThread();
		if(session!=null&&!session.nioSelector.equals(reactorThread.getSelector())){
			session.channelKey.cancel();
			session.nioSelector = reactorThread.getSelector();
			session.channelKey = session.channel.register(session.nioSelector, SelectionKey.OP_READ, session);
		}
		return session;
	}

	public MySQLDataSource(MySQLBean config, boolean islaveNode) {
		this.activeSize = new AtomicInteger(0);
		this.mysqlBean = config;
		this.name = config.getHostName();
		this.slaveNode = islaveNode;
	}

	public boolean createMySQLSession(MycatSession mycatSession, BufferPool bufferPool, Selector selector, SchemaBean schema, AsynTaskCallBack<MySQLSession> callback) throws IOException {
		if (mycatSession != null) {
			int newSize = activeSize.incrementAndGet();
			if (newSize > mysqlBean.getMaxCon()) {
				//超过最大连接数
				activeSize.decrementAndGet();
				return false;
			}
		}

		BackendConCreateTask authProcessor = new BackendConCreateTask(bufferPool, selector, this, schema);
		authProcessor.setCallback(callback);
		if (mycatSession != null) {
			mycatSession.setCurNIOHandler(authProcessor);
		}
		return true;
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
			reactorThread.addNIOJob(() -> {
				try {
					createMySQLSession(null, reactorThread.getBufPool(),
							reactorThread.getSelector(), null,
							(optSession, sender, exeSucces, retVal) -> {
								if (exeSucces) {
									int curSize = activeSize.incrementAndGet();
									//设置当前连接 读写分离属性
									optSession.setDefaultChannelRead(this.slaveNode);
									if (curSize == 1) {
										BackendCharsetReadTask backendCharsetReadTask =
												new BackendCharsetReadTask(optSession, this);
										optSession.setCurNIOHandler(backendCharsetReadTask);
										backendCharsetReadTask.readCharset();
									}
									optSession.change2ReadOpts();
									sessionQueue.add(optSession);
								}
							});
				} catch (IOException e) {
					LOGGER.error("error to create mySQL connection", e);
				}
			});
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

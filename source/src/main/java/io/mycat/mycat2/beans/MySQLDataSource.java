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
	private final int size;
	private final MySQLBean mysqlBean;
	private final ConMap conMap = new ConMap();
	private long heartbeatRecoveryTime;
	private boolean slaveNode;

	public MySQLDataSource(MySQLBean config, boolean islaveNode) {
		this.size = config.getMaxCon();
		this.mysqlBean = config;
		this.name = config.getHostName();
		this.slaveNode = islaveNode;

	}

	// public MySQLBackendConnection createNewConnection(String reactor, String
	// schema, MySQLFrontConnection mySQLFrontConnection, BackConnectionCallback
	// userCallback) throws IOException {
	// MySQLBackendConnection con = factory.make(this, reactor, schema,
	// mySQLFrontConnection, userCallback);
	// this.conMap.getSchemaConQueue(schema).getAutoCommitCons().add(con);
	// return con;
	// }

	public boolean isSlaveNode() {
		return slaveNode;
	}

	public void setSlaveNode(boolean slaveNode) {
		this.slaveNode = slaveNode;
	}

	public int getSize() {
		return size;
	}

	public String getName() {
		return name;
	}

	public boolean initSource() {
		int initSize = this.mysqlBean.getMinCon();
		LOGGER.info("init backend myqsl source ,create connections total " + initSize + " for " + mysqlBean);
		// Set<String> reactos =
		// SQLEngineCtx.INSTANCE().getReactorMap().keySet();
		// Iterator<String> itor = reactos.iterator();
		// for (int i = 0; i < initSize; i++) {
		// try {
		// String actorName = null;
		// if (!itor.hasNext()) {
		// itor = reactos.iterator();
		// }
		// actorName = itor.next();
		// this.createNewConnectionOnReactor(actorName,
		// this.mysqlBean.getDefaultSchema(), null, null);
		// } catch (Exception e) {
		// LOGGER.warn(" init connection error.", e);
		// }
		// }
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
				.append("size=").append(size).append(',').append("heartbeatRecoveryTime=").append(heartbeatRecoveryTime)
				.append(',').append("slaveNode=").append(slaveNode).append(',').append("mysqlBean=").append(mysqlBean)
				.append(']');
		return (sbuf.toString());
	}

}

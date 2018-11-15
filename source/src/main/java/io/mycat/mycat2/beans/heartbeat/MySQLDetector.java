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
package io.mycat.mycat2.beans.heartbeat;

import io.mycat.mycat2.net.MainMycatNIOHandler;
import io.mycat.mycat2.tasks.BackendHeartbeatTask;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.util.TimeUtil;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mycat
 */
public class MySQLDetector {
	
	private MySQLHeartbeat heartbeat;
	
	private long heartbeatTimeout;
	private final AtomicBoolean isQuit;
	private volatile long lastSendQryTime;
	private volatile long lasstReveivedQryTime;
	
	public MySQLDetector(MySQLHeartbeat heartbeat) {
		this.heartbeat = heartbeat;
		this.isQuit = new AtomicBoolean(false);
	}

	public MySQLHeartbeat getHeartbeat() {
		return heartbeat;
	}

	public long getHeartbeatTimeout() {
		return heartbeatTimeout;
	}

	public void setHeartbeatTimeout(long heartbeatTimeout) {
		this.heartbeatTimeout = heartbeatTimeout;
	}

	public boolean isHeartbeatTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastSendQryTime,
				lasstReveivedQryTime) + heartbeatTimeout;
	}

	public long getLastSendQryTime() {
		return lastSendQryTime;
	}

	public long getLasstReveivedQryTime() {
		return lasstReveivedQryTime;
	}

	public void heartbeat() throws IOException {
		lastSendQryTime = System.currentTimeMillis();
		MycatReactorThread reactor = (MycatReactorThread)Thread.currentThread();
		MySQLDetector detector = this;

		reactor.getMysqlSession(heartbeat.getSource(), (optSession, sender, exeSucces, rv) -> {
			if (exeSucces) {
				BackendHeartbeatTask heartbeatTask = new BackendHeartbeatTask(optSession,detector);
				heartbeatTask.setCallback((mysqlsession, sder, isSucc, rsmsg) -> {
					//恢复默认的Handler
					optSession.setCurNIOHandler(MainMycatNIOHandler.INSTANCE);

				});
				optSession.setCurNIOHandler(heartbeatTask);
				heartbeatTask.doHeartbeat();
			}else{
				if(optSession!=null){
					optSession.close(false, ((ErrorPacket)rv).message);
				}
				//连接创建 失败. 如果是主节点，需要重试.并在达到重试次数后,通知集群
				if(heartbeat.incrErrorCount() < heartbeat.getSource().getDsMetaBean().getMaxRetryCount()){
					heartbeat();
				}else{
                    heartbeat.setResult(DBHeartbeat.ERROR_STATUS,
                            this,
										heartbeat.getSource().getDsMetaBean().getIp()+":"+heartbeat.getSource().getDsMetaBean().getPort()
										+" connection timeout!!");
				}
			}
		});


    }

	public void quit() {
		isQuit.lazySet(false);
	}
	
	public boolean isQuit() {
		return isQuit.get();
	}

	public void setLasstReveivedQryTime(long lasstReveivedQryTime) {
		this.lasstReveivedQryTime = lasstReveivedQryTime;
	}
}

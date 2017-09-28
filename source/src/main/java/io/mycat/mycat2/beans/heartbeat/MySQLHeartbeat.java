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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

import io.mycat.mycat2.beans.conf.ClusterConfig;
import io.mycat.proxy.man.cmds.LeaderNotifyPacketCommand;
import io.mycat.proxy.man.packet.LeaderNotifyPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.CheckResult;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.conf.HeartbeatConfig;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyRuntime;

/**
 * @author mycat
 */
public class MySQLHeartbeat extends DBHeartbeat {

	
	public static final Logger logger = LoggerFactory.getLogger(MySQLHeartbeat.class);

	private final MySQLMetaBean source;

	private final ReentrantLock lock;

	private MySQLDetector detector;

	public MySQLHeartbeat(MySQLMetaBean source,int status) {
		this.source = source;
		this.lock = new ReentrantLock(false);
		this.status = status;
	}

	public MySQLMetaBean getSource() {
		return source;
	}

	public MySQLDetector getDetector() {
		return detector;
	}

	public long getTimeout() {
		MySQLDetector detector = this.detector;
		if (detector == null) {
			return -1L;
		}
		return detector.getHeartbeatTimeout();
	}

	public String getLastActiveTime() {
		MySQLDetector detector = this.detector;
		if (detector == null) {
			return null;
		}
		long t = detector.getLasstReveivedQryTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(t));
	}

	public void start() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			isStop.compareAndSet(true, false);
			super.status = DBHeartbeat.OK_STATUS;
		} finally {
			lock.unlock();
		}
	}

	public void stop() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (isStop.compareAndSet(false, true)) {
				if (isChecking.get()) {
					// nothing
				} else {
					MySQLDetector detector = this.detector;
					if (detector != null) {
						isChecking.set(false);
					}
				}
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * execute heart beat
	 */
	public void heartbeat() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (isChecking.compareAndSet(false, true)) {
				logger.debug("backend mysql heartbeat begin.{}:{}", source.getDsMetaBean().getIp(), source.getDsMetaBean().getPort());
				MySQLDetector detector = this.detector;
				if (detector == null || detector.isQuit()) {
					try {
						detector = new MySQLDetector(this);
						detector.heartbeat();
					} catch (Exception e) {
						logger.error(source.toString(), e);
						setResult(ERROR_STATUS, detector, null);
						return;
					}
					this.detector = detector;
				} else {
					try {
						detector.heartbeat();
					} catch (IOException e) {
						logger.error(source.toString(), e);
						setResult(ERROR_STATUS, detector, null);
						return;
					}
				}
			} else {
				MySQLDetector detector = this.detector;
				if (detector != null) {
					if (detector.isQuit()) {
						isChecking.compareAndSet(true, false);
					} else if (detector.isHeartbeatTimeout()) {
						setResult(TIMEOUT_STATUS, detector, null);
					}
				}
			}
		} finally {
			lock.unlock();
		}
	}

	public void setResult(int result, MySQLDetector detector, String msg) {
		this.isChecking.set(false);
		switch (result) {
			case OK_STATUS:
				setOk(detector);
				break;
			case ERROR_STATUS:
				setError(detector,msg);
				break;
			case TIMEOUT_STATUS:
				setTimeout(detector);
				break;
			case ERROR_CONF: //配置错误时,停止心跳。
				//TODO 配置错误,是否通知集群
	//			System.exit(0);
				break;
		}
	}

	private void setOk(MySQLDetector detector) {
		switch (status) {
			case DBHeartbeat.TIMEOUT_STATUS:
				this.status = DBHeartbeat.INIT_STATUS;
				this.errorCount = 0;
				if (!isStop.get()) {
					heartbeat();// timeout, heart beat again
				}
				break;
			case DBHeartbeat.INIT_STATUS:
				logger.info("current repl status [INIT_STATUS ---> OK_STATUS]. update lastSwitchTime .{}:{}", source.getDsMetaBean().getIp(), source.getDsMetaBean().getPort());
				MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
				HeartbeatConfig heartbeatConfig = conf.getConfig(ConfigEnum.HEARTBEAT);
				source.getRepBean().setLastSwitchTime(System.currentTimeMillis() - heartbeatConfig.getHeartbeat().getMinSwitchtimeInterval());
			case DBHeartbeat.OK_STATUS:
			default:
				this.status = OK_STATUS;
				this.errorCount = 0;
		}
		//当心跳成功，同时字符集未加载过，则加载字符集，如果是集群模式，需要通知集群加载字符集
		if (this.status == OK_STATUS && !source.charsetLoaded) {
			try {
			    ClusterConfig clusterConfig = ProxyRuntime.INSTANCE.getConfig().getConfig(ConfigEnum.CLUSTER);
			    if (clusterConfig.getCluster().isEnable()) {
                    LeaderNotifyPacket pkg = new LeaderNotifyPacket(LeaderNotifyPacket.LOAD_CHARACTER,
                            source.getRepBean().getReplicaBean().getName(), Integer.toString(source.getIndex()));
                    LeaderNotifyPacketCommand.INSTANCE.sendNotifyCmd(pkg);
                }
				source.init();
			} catch (IOException e) {
				logger.error("error to init datasource for MySQLMetaBean {}", source);
			}
		}
	}

	private void setError(MySQLDetector detector, String msg) {
		// should continues check error status
		if (++errorCount < source.getDsMetaBean().getMaxRetryCount()) {
            if (detector != null && !detector.isQuit()) {
                heartbeat(); // error count not enough, heart beat again
            }
		} else {
			if (source.isSlaveNode()) {
				logger.error(msg);
			} else {
				//写节点 尝试多次次失败后, 需要通知集群
				logger.warn("heartbeat to backend session error, notify the cluster if needed");

				MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
				HeartbeatConfig heartbeatConfig = conf.getConfig(ConfigEnum.HEARTBEAT);
				long curTime = System.currentTimeMillis();
				long minSwitchTimeInterval = heartbeatConfig.getHeartbeat().getMinSwitchtimeInterval();
				if (((curTime - source.getRepBean().getLastSwitchTime()) < minSwitchTimeInterval)
						|| (curTime - source.getRepBean().getLastInitTime()) < minSwitchTimeInterval) {
					logger.warn("the Minimum time interval for switchSource is {} seconds.", minSwitchTimeInterval / 1000L);
					return;
				}
				
				int next = source.getRepBean().getNextIndex();
				CheckResult result = source.getRepBean().switchDataSourcecheck(next);
				
				if(result.isSuccess()){
					
					if (next == -1) {
						logger.error("all metaBean in replica is invalid !!!");
					} else {
						String repName = source.getRepBean().getReplicaBean().getName();
						ProxyRuntime.INSTANCE.prepareSwitchDataSource(repName, next,true);
					}
				}else{
					logger.error(result.getMsg());
				}
			}
            this.status = ERROR_STATUS;
            this.errorCount = 0;
        }
	}

	private void setTimeout(MySQLDetector detector) {
		this.isChecking.set(false);
		status = DBHeartbeat.TIMEOUT_STATUS;
	}

//	/**
//	 * switch data source
//	 */
//	private void switchSourceIfNeed(String reason) {
//		int switchType = source.getHostConfig().getSwitchType();
//		if (switchType == DataHostConfig.NOT_SWITCH_DS) {
//			if (LOGGER.isDebugEnabled()) {
//				LOGGER.debug("not switch datasource ,for switchType is "
//						+ DataHostConfig.NOT_SWITCH_DS);
//				return;
//			}
//			return;
//		}
//		PhysicalDBPool pool = this.source.getDbPool();
//		int curDatasourceHB = pool.getSource().getHeartbeat().getStatus();
//		// read node can't switch ,only write node can switch
//		if (pool.getWriteType() == PhysicalDBPool.WRITE_ONLYONE_NODE
//				&& !source.isReadNode()
//				&& curDatasourceHB != DBHeartbeat.OK_STATUS
//				&& pool.getSources().length > 1) {
//			synchronized (pool) {
//				// try to see if need switch datasource
//				curDatasourceHB = pool.getSource().getHeartbeat().getStatus();
//				if (curDatasourceHB != DBHeartbeat.INIT_STATUS && curDatasourceHB != DBHeartbeat.OK_STATUS) {
//					int curIndex = pool.getActivedIndex();
//					int nextId = pool.next(curIndex);
//					PhysicalDatasource[] allWriteNodes = pool.getSources();
//					while (true) {
//						if (nextId == curIndex) {
//							break;
//						}
//						PhysicalDatasource theSource = allWriteNodes[nextId];
//						DBHeartbeat theSourceHB = theSource.getHeartbeat();
//						int theSourceHBStatus = theSourceHB.getStatus();
//						if (theSourceHBStatus == DBHeartbeat.OK_STATUS) {
//							if (switchType == DataHostConfig.SYN_STATUS_SWITCH_DS) {
//								if (Integer.valueOf(0).equals( theSourceHB.getSlaveBehindMaster())) {
//									LOGGER.info("try to switch datasource ,slave is synchronized to master " + theSource.getConfig());
//									pool.switchSource(nextId, true, reason);
//									break;
//								} else {
//									LOGGER.warn("ignored  datasource ,slave is not  synchronized to master , slave behind master :"
//											+ theSourceHB.getSlaveBehindMaster()
//											+ " " + theSource.getConfig());
//								}
//							} else {
//								// normal switch
//								LOGGER.info("try to switch datasource ,not checked slave synchronize status " + theSource.getConfig());
//								pool.switchSource(nextId, true, reason);
//                                break;
//							}
//
//						}
//						nextId = pool.next(nextId);
//					}
//
//				}
//			}
//		}
//	}
}

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
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.mycat2.beans;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import io.mycat.mycat2.beans.conf.DatasourceMetaBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.heartbeat.DBHeartbeat;
import io.mycat.mycat2.beans.heartbeat.MySQLHeartbeat;
import io.mycat.mycat2.tasks.BackendCharsetReadTask;
import io.mycat.mycat2.tasks.BackendGetConnectionTask;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.TimeUtil;

/**
 * 后端mysql连接元数据类，对应datasource.xml配置中的mysql元数据信息
 * @author wuzhihui
 */
public class MySQLMetaBean {
	private static final Logger logger = LoggerFactory.getLogger(MySQLMetaBean.class);

	private DatasourceMetaBean dsMetaBean;
    private volatile boolean slaveNode = true; // 默认为slave节点
    private volatile long heartbeatRecoveryTime;  // 心跳暂停时间
    private DBHeartbeat heartbeat;
    private MySQLRepBean repBean;

    private int slaveThreshold = -1;

    public boolean charsetLoaded = false;

    /** collationIndex 和 charsetName 的映射 */
    public static final Map<Integer, String> INDEX_TO_CHARSET = new HashMap<>();
    /** charsetName 到 默认collationIndex 的映射 */
    public static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();

    public boolean init(MySQLRepBean repBean,long maxwaitTime,int status) throws IOException {
    	logger.info("init backend myqsl source ,create connections total " + dsMetaBean.getMinCon() + " for " + dsMetaBean.getHostName() + " index :" + repBean.getWriteIndex());

    	this.repBean = repBean;
    	heartbeat = new MySQLHeartbeat(this,status);
    	ProxyRuntime runtime = ProxyRuntime.INSTANCE;
        MycatReactorThread[] reactorThreads = (MycatReactorThread[]) runtime.getReactorThreads();
        int reactorSize = runtime.getNioReactorThreads();
        CopyOnWriteArrayList<MySQLSession > list = new CopyOnWriteArrayList<MySQLSession >();
        BackendGetConnectionTask getConTask = new BackendGetConnectionTask(list, dsMetaBean.getMinCon());
        for (int i = 0; i < dsMetaBean.getMinCon(); i++) {
        	MycatReactorThread reactorThread = reactorThreads[i % reactorSize];
            reactorThread.addNIOJob(() -> {
                try {
                    reactorThread.createSession(this, null, (optSession, sender, exeSucces, retVal) -> {
                        if (exeSucces) {
                            //设置当前连接 读写分离属性
                            optSession.setDefaultChannelRead(this.isSlaveNode());
                            if (this.charsetLoaded == false) {
                                this.charsetLoaded = true;
                                BackendCharsetReadTask backendCharsetReadTask = new BackendCharsetReadTask(optSession, this,getConTask);
                                optSession.setCurNIOHandler(backendCharsetReadTask);
                                backendCharsetReadTask.readCharset();
                            } else {
                            	getConTask.finished(optSession,sender,exeSucces,retVal);
							}
							optSession.change2ReadOpts();
							reactorThread.addMySQLSession(this, optSession);
						} else {
							this.charsetLoaded = false;
							getConTask.finished(optSession,sender,exeSucces,retVal);
                        }
                    });
                } catch (IOException ignore) {
                }
            });
        }
        
        long timeOut = System.currentTimeMillis() + maxwaitTime;

		// waiting for finish
		while (!getConTask.finished() && (System.currentTimeMillis() < timeOut)) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				logger.error("initError", e);
			}
		}
		logger.info("init result : {}",getConTask.getStatusInfo());
		return !list.isEmpty();
    }
    
	public void doHeartbeat() {
		// 未到预定恢复时间，不执行心跳检测。
		if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
			return;
		}
		
		try {
			heartbeat.heartbeat();
		} catch (Exception e) {
			logger.error(dsMetaBean.getHostName() + " heartbeat error.", e);
		}
	}
	
	/**
	 * 清理旧
	 * @param reason
	 */
	public void clearCons(String reason) {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		MycatReactorThread[] reactorThreads = (MycatReactorThread[]) runtime.getReactorThreads();
		Arrays.stream(reactorThreads).forEach(f -> f.clearMySQLMetaBeanSession(this,reason));
	}
	
	/**
	 * 检查当前是否可用
	 * @return
	 */	
	public boolean canSelectAsReadNode() {
		int slaveBehindMaster = heartbeat.getSlaveBehindMaster();
		int dbSynStatus = heartbeat.getDbSynStatus();
		
		if (!isAlive()){
			return false;
		}
		
		if (dbSynStatus == DBHeartbeat.DB_SYN_ERROR) {
			return false;
		}
		boolean isSync = dbSynStatus == DBHeartbeat.DB_SYN_NORMAL;
		boolean isNotDelay = (slaveThreshold >= 0) ? (slaveBehindMaster < slaveThreshold) : true;
		return isSync && isNotDelay;
	}

	public DatasourceMetaBean getDsMetaBean() {
		return dsMetaBean;
	}

	public void setDsMetaBean(DatasourceMetaBean dsMetaBean) {
		this.dsMetaBean = dsMetaBean;
	}

	public boolean isSlaveNode() {
        return slaveNode;
    }

    public void setSlaveNode(boolean slaveNode) {
        this.slaveNode = slaveNode;
    }

	public MySQLRepBean getRepBean() {
		return repBean;
	}

	public void setRepBean(MySQLRepBean repBean) {
		this.repBean = repBean;
	}

	public int getSlaveThreshold() {
		return slaveThreshold;
	}

	public void setSlaveThreshold(int slaveThreshold) {
		this.slaveThreshold = slaveThreshold;
	}

	public DBHeartbeat getHeartbeat() {
		return heartbeat;
	}

	/**
	 * 当前节点是否存活
	 * @return
	 */
	public boolean isAlive() {
		return heartbeat.getStatus() == DBHeartbeat.OK_STATUS;
	}

    @Override
    public String toString() {
        return "MySQLMetaBean [hostName=" + dsMetaBean.getHostName() + ", ip=" + dsMetaBean.getIp() + ", port=" + dsMetaBean.getPort() + ", user=" + dsMetaBean.getUser() + ", password="
                + dsMetaBean.getPassword() + ", maxCon=" + dsMetaBean.getMaxCon() + ", minCon=" + dsMetaBean.getMinCon() + ", slaveNode=" + slaveNode + "]";
    }
}

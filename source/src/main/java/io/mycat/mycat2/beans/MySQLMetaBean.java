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
	
	//默认的重试次数
	private static final int MAX_RETRY_COUNT = 5;  
	
	private static final Logger logger = LoggerFactory.getLogger(MySQLMetaBean.class);
    private String hostName; 
    private String ip;
    private int port;
    private String user;
    private String password;
    private int maxCon = 1000;
    private int minCon = 1;
    private volatile boolean slaveNode = true; // 默认为slave节点
    private volatile long heartbeatRecoveryTime;  // 心跳暂停时间
    private DBHeartbeat heartbeat;
    private MySQLRepBean repBean;
    
	private int maxRetryCount = MAX_RETRY_COUNT;  //重试次数
    
    private int slaveThreshold = -1;

    public boolean charsetLoaded = false;

    /** collationIndex 和 charsetName 的映射 */
    public final Map<Integer, String> INDEX_TO_CHARSET = new HashMap<>();
    /** charsetName 到 默认collationIndex 的映射 */
    public final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();

    public boolean init(MySQLRepBean repBean,long maxwaitTime) throws IOException {
  	
    	logger.info("init backend myqsl source ,create connections total " + minCon + " for " + hostName + " index :" + repBean.getWriteIndex());

    	this.repBean = repBean;
    	heartbeat = new MySQLHeartbeat(this,DBHeartbeat.INIT_STATUS);
    	ProxyRuntime runtime = ProxyRuntime.INSTANCE;
        MycatReactorThread[] reactorThreads = (MycatReactorThread[]) runtime.getReactorThreads();
        int reactorSize = runtime.getNioReactorThreads();
        CopyOnWriteArrayList<MySQLSession > list = new CopyOnWriteArrayList<MySQLSession >();
        BackendGetConnectionTask getConTask = new BackendGetConnectionTask(list, minCon);
        for (int i = 0; i < minCon; i++) {
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
                            }else{
                            	getConTask.finished(optSession,sender,exeSucces,retVal);
                            }
                            optSession.change2ReadOpts();
                            reactorThread.addMySQLSession(this, optSession);
                        }else{
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
			logger.error(hostName + " heartbeat error.", e);
		}
	}
	
	/**
	 * 清理旧
	 * @param reason
	 */
	public void clearCons(String reason) {
		ProxyRuntime runtime = ProxyRuntime.INSTANCE;
		MycatReactorThread[] reactorThreads = (MycatReactorThread[]) runtime.getReactorThreads();
		Arrays.stream(reactorThreads).forEach(f->f.clearMySQLMetaBeanSession(this,reason));
	}
	
	/**
	 * 检查当前是否可用
	 * @return
	 */	
	public boolean canSelectAsReadNode() {
		
		int slaveBehindMaster = heartbeat.getSlaveBehindMaster();
		int dbSynStatus = heartbeat.getDbSynStatus();
		
		if(!isAlive()){
			return false;
		}
		
		if (dbSynStatus == DBHeartbeat.DB_SYN_ERROR) {
			return false;
		}
		boolean isSync = dbSynStatus == DBHeartbeat.DB_SYN_NORMAL;
		boolean isNotDelay = (slaveThreshold >=0)?(slaveBehindMaster < slaveThreshold):true;		
		return isSync && isNotDelay;
	}

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(int maxCon) {
        this.maxCon = maxCon;
    }

    public int getMinCon() {
        return minCon;
    }

    public void setMinCon(int minCon) {
        this.minCon = minCon;
    }

    public boolean isSlaveNode() {
        return slaveNode;
    }

    public void setSlaveNode(boolean slaveNode) {
        this.slaveNode = slaveNode;
    }

    @Override
    public String toString() {
        return "MySQLMetaBean [hostName=" + hostName + ", ip=" + ip + ", port=" + port + ", user=" + user + ", password="
                + password + ", maxCon=" + maxCon + ", minCon=" + minCon + ", slaveNode=" + slaveNode + "]";
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

	public int getMaxRetryCount() {
		return maxRetryCount;
	}

	public void setMaxRetryCount(int maxRetryCount) {
		this.maxRetryCount = maxRetryCount;
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
}

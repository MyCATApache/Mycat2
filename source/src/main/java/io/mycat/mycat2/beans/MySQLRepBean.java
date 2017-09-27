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
package io.mycat.mycat2.beans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import io.mycat.mycat2.beans.conf.DatasourceConfig;
import io.mycat.mycat2.beans.conf.ReplicaBean;
import io.mycat.mycat2.beans.conf.ReplicaIndexConfig;
import io.mycat.mycat2.beans.conf.ReplicaBean.RepTypeEnum;
import io.mycat.proxy.ConfigEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.heartbeat.DBHeartbeat;
import io.mycat.mysql.Alarms;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.MyCluster;

/**
 * 表示一組MySQL Server复制集群，如主从或者多主
 *
 * @author wuzhihui
 */
public class MySQLRepBean {
	private static Logger logger = LoggerFactory.getLogger(MySQLRepBean.class);

	private ReplicaBean replicaBean;
    private String slaveIDs;   // 在线数据迁移 虚拟从节点
    private boolean tempReadHostAvailable = false;  //如果写服务挂掉, 临时读服务是否继续可用

    private List<MySQLMetaBean> metaBeans = new ArrayList<>();
    protected final ReentrantLock switchLock = new ReentrantLock();
    public AtomicBoolean switchResult = new AtomicBoolean();

    private volatile int writeIndex = 0; //主节点默认为0
    private long lastSwitchTime;
    private long lastInitTime;  //最后一次初始化时间
	
    public void initMaster() {
        // 根据配置replica-index的配置文件修改主节点
        MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
		ReplicaIndexConfig repIndexConfig = conf.getConfig(ConfigEnum.REPLICA_INDEX);
        Integer repIndex = repIndexConfig.getReplicaIndexes().get(replicaBean.getName());
        if (repIndex != null && checkIndex(repIndex)) {
            writeIndex = repIndex;
        } else {
        	writeIndex = 0;
        }
		replicaBean.getMysqls().forEach(dsMetaBean -> {
			MySQLMetaBean metaBean = new MySQLMetaBean();
			metaBean.setDsMetaBean(dsMetaBean);
			metaBeans.add(metaBean);
		});
		metaBeans.get(writeIndex).setSlaveNode(false);
    }
    
	public void doHeartbeat() {
		if (metaBeans.get(writeIndex) == null) {
			return;
		}

		for (MySQLMetaBean source : metaBeans) {
			if (source != null) {
				source.doHeartbeat();
			} else {
				StringBuilder s = new StringBuilder();
				s.append(Alarms.DEFAULT).append(replicaBean.getName()).append(" current dataSource is null!");
				logger.error(s.toString());
			}
		}
	}
    
    private boolean checkIndex(int newIndex){
    	return newIndex >= 0 && newIndex < metaBeans.size();
    }
    
    public int getNextIndex(){    	
    	MySQLMetaBean metaBean = metaBeans.stream().skip(writeIndex + 1).findFirst().orElse(null);
    	if (metaBean!=null){
    		return metaBeans.indexOf(metaBean);
    	} else {
    		metaBean = metaBeans.stream().limit(writeIndex).findFirst().orElse(null);
    		if(metaBean!=null){
    			return metaBeans.indexOf(metaBean);
    		}
    	}
    	return -1;
    }
    
	/**
	 * 准备 主从切换前的检查
	 * @param replBean
	 * @param writeIndex
	 * @return
	 */
	public CheckResult switchDataSourcecheck(int newIndex){
		String errmsg = null;
		CheckResult result = new CheckResult(true);
		
		if(RepTypeEnum.SINGLE_NODE.equals(getReplicaBean().getRepType())){
			errmsg = " repl type is "+RepTypeEnum.SINGLE_NODE.name() + ", switchDatasource is not supported";
			logger.warn(errmsg);
			result.setSuccess(false);
			result.setMsg(errmsg);
		}else if(!checkIndex(newIndex)){
			errmsg = "not switch datasource ,writeIndex  out of range. writeIndex is " + newIndex;
			logger.warn(errmsg);
			result.setSuccess(false);
			result.setMsg(errmsg);
		}else if(newIndex==writeIndex){
			errmsg = "not switch datasource ,writeIndex == newIndex .newIndex is " + newIndex;
			logger.warn(errmsg);
			result.setSuccess(false);
			result.setMsg(errmsg);
		}
		return result;
	}
    
	public void switchSource(int newIndex,long maxwaittime) {
		if (replicaBean.getSwitchType() == ReplicaBean.RepSwitchTypeEnum.NOT_SWITCH) {
			logger.debug("not switch datasource ,for switchType is {}", ReplicaBean.RepSwitchTypeEnum.NOT_SWITCH);
			switchResult.set(false);
			return;
		}
		
		if(!checkIndex(newIndex)){
			logger.debug("not switch datasource ,writeIndex  out of range. writeIndex is {}",newIndex);
			switchResult.set(false);
			return;
		}
		
		
		final ReentrantLock lock = this.switchLock;
		lock.lock();
		
		try {
			
			switchResult.set(false);

			int current = writeIndex;
			if (current != newIndex) {
				
				String reason = "switch datasource";
				
				// init again
				MySQLMetaBean newWriteBean = metaBeans.get(newIndex);
				newWriteBean.clearCons(reason);
				newWriteBean.init(this,maxwaittime,getDataSourceInitStatus());
				
				// clear all connections
				MySQLMetaBean oldMetaBean = metaBeans.get(current);
				oldMetaBean.clearCons(reason);
				// write log
				logger.warn(switchMessage(current, newIndex, reason));
				
				switchResult.set(true);
				
				// switch index
				writeIndex = newIndex;
				oldMetaBean.setSlaveNode(true);
				newWriteBean.setSlaveNode(false);
				
				lastInitTime = System.currentTimeMillis();
			}else{
				logger.debug("not switch datasource ,writeIndex == newIndex .newIndex is {}",newIndex);
			}
		}catch (IOException e) {
			e.printStackTrace();
			switchResult.set(false);
		}finally {
			lock.unlock();
		}
	}
	
	public int getDataSourceInitStatus(){
		int initstatus = DBHeartbeat.OK_STATUS;
		MyCluster myCluster = ProxyRuntime.INSTANCE.getMyCLuster();
		
		if(myCluster==null||myCluster.getMyLeader()==myCluster.getMyNode()){
			initstatus = DBHeartbeat.INIT_STATUS;
		}
		return initstatus;
	}
	
	private String switchMessage(int current, int newIndex, String reason) {
		StringBuilder s = new StringBuilder();
		s.append("[Host=").append(replicaBean.getName()).append(",result=[").append(current).append("->");
		s.append(newIndex).append("],reason=").append(reason).append(']');
		return s.toString();
	}

    /**
     * 得到当前用于写的MySQLMetaBean
     */
    private MySQLMetaBean getCurWriteMetaBean() {
        return metaBeans.get(writeIndex).isAlive() ? metaBeans.get(writeIndex) : null;
    }

    public MySQLMetaBean getBalanceMetaBean(boolean runOnSlave){
    	if(ReplicaBean.RepTypeEnum.SINGLE_NODE == replicaBean.getRepType()||!runOnSlave){
    		return getCurWriteMetaBean();
    	}
    	
    	MySQLMetaBean datas = null;
    	
		switch(replicaBean.getBalanceType()){
			case BALANCE_ALL:
				datas = getLBReadWriteMetaBean();
				break;
			case BALANCE_ALL_READ:
				datas = getLBReadMetaBean();
				//如果从节点不可用,从主节点获取连接
				if(datas==null){
					logger.debug("all slaveNode is Unavailable. use master node for read . balance type is {}", replicaBean.getBalanceType());
					datas = getCurWriteMetaBean();
				}
				break;
			case BALANCE_NONE:
				datas = getCurWriteMetaBean();
				break;
			default:
				logger.debug("current balancetype is not supported!! [{}], use writenode connection .", replicaBean.getBalanceType());
				datas = getCurWriteMetaBean();
				break;
		}
		return datas;
    }

    /**
     * 得到当前用于读的MySQLMetaBean（负载均衡模式，如果支持）
     * 当前读写节点都承担负载
     */
    private MySQLMetaBean getLBReadWriteMetaBean() {
    	List<MySQLMetaBean> result = metaBeans.stream()
    			.filter(f -> f.canSelectAsReadNode())
    			.collect(Collectors.toList());
        return result.isEmpty()?null:result.get(ThreadLocalRandom.current().nextInt(result.size()));
    }

    /**
     * 只有当前读节点 承担负载
     * @return
     */
    private MySQLMetaBean getLBReadMetaBean(){
    	List<MySQLMetaBean> result = metaBeans.stream()
    			.filter(f -> f.isSlaveNode() && f.canSelectAsReadNode())
    			.collect(Collectors.toList());
    	return result.isEmpty() ? null : result.get(ThreadLocalRandom.current().nextInt(result.size()));
    }

	public ReplicaBean getReplicaBean() {
		return replicaBean;
	}

	public void setReplicaBean(ReplicaBean replicaBean) {
		this.replicaBean = replicaBean;
	}

	public List<MySQLMetaBean> getMetaBeans() {
		return metaBeans;
	}

	public void setMetaBeans(List<MySQLMetaBean> metaBeans) {
		this.metaBeans = metaBeans;
	}

	public String getSlaveIDs() {
		return slaveIDs;
	}

	public void setSlaveIDs(String slaveIDs) {
		this.slaveIDs = slaveIDs;
	}

	public boolean isTempReadHostAvailable() {
		return tempReadHostAvailable;
	}

	public void setTempReadHostAvailable(boolean tempReadHostAvailable) {
		this.tempReadHostAvailable = tempReadHostAvailable;
	}

	public int getWriteIndex() {
		return writeIndex;
	}

	public AtomicBoolean getSwitchResult() {
		return switchResult;
	}

	public void setSwitchResult(boolean flag) {
		this.switchResult.set(flag);
	}

	public long getLastSwitchTime() {
		return lastSwitchTime;
	}

	public void setLastSwitchTime(long lastSwitchTime) {
		this.lastSwitchTime = lastSwitchTime;
	}

	public long getLastInitTime() {
		return lastInitTime;
	}
}

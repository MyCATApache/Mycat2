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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatConfig;
import io.mycat.mycat2.beans.MySQLRepBean.BalanceTypeEnum;
import io.mycat.mysql.Alarms;
import io.mycat.proxy.ProxyRuntime;

/**
 * 表示一組MySQL Server复制集群，如主从或者多主
 *
 * @author wuzhihui
 */
public class MySQLRepBean {
	
	private static Logger logger = LoggerFactory.getLogger(MySQLRepBean.class);
	
	private final static String SINGLENODE_hearbeatSQL = "select 1";
	private final static String MASTER_SLAVE_hearbeatSQL = "show slave status";
	private final static String MASTER_SLAVE_GTID_hearbeatSQL = "show slave status";
	private final static String GARELA_CLUSTER_hearbeatSQL = "show status like 'wsrep%'";
	private final static String GROUP_REPLICATION_hearbeatSQL = "show slave status";
	
	private static final String[] MYSQL_SLAVE_STAUTS_COLMS = new String[] {
			"Seconds_Behind_Master", 
			"Slave_IO_Running", 
			"Slave_SQL_Running",
			"Slave_IO_State",
			"Master_Host",
			"Master_User",
			"Master_Port", 
			"Connect_Retry",
			"Last_IO_Error"};

	private static final String[] MYSQL_CLUSTER_STAUTS_COLMS = new String[] {
			"Variable_name",
			"Value"};
	
    public enum RepTypeEnum {
    	
    	SINGLENODE(SINGLENODE_hearbeatSQL,MYSQL_SLAVE_STAUTS_COLMS),                //单一节点 
        MASTER_SLAVE(MASTER_SLAVE_hearbeatSQL,MYSQL_SLAVE_STAUTS_COLMS),            //普通主从
        GARELA_CLUSTER(GARELA_CLUSTER_hearbeatSQL,MYSQL_CLUSTER_STAUTS_COLMS),        //普通基于garela cluster 集群
        GROUP_REPLICATION(GROUP_REPLICATION_hearbeatSQL,MYSQL_SLAVE_STAUTS_COLMS);  //基于 MGR  集群
    	
    	private String hearbeatSQL;
    	
    	String[] fetchColms;
    	
        RepTypeEnum(String hearbeatSQL,String[] fetchColms) {
        	this.hearbeatSQL = hearbeatSQL;
        	this.fetchColms  = fetchColms;
        }

        public String getHearbeatSQL() {
			return hearbeatSQL;
		}
        
        public String[] getFetchColms() {
			return fetchColms;
		}
    }

    public enum RepSwitchTypeEnum {
    	NOT_SWITCH, 
    	DEFAULT_SWITCH,
    	SYN_STATUS_SWITCH,
    	CLUSTER_STATUS_SWITCH;
        RepSwitchTypeEnum() {}
    }

    public enum BalanceTypeEnum{
    	BALANCE_ALL,
    	BALANCE_ALL_READ,
    	BALANCE_NONE;
    	BalanceTypeEnum() {}
    }

    private String name;
    private RepTypeEnum type;
    private RepSwitchTypeEnum switchType = RepSwitchTypeEnum.DEFAULT_SWITCH;
    private BalanceTypeEnum balance = BalanceTypeEnum.BALANCE_NONE;
    private List<MySQLMetaBean> mysqls;
    private String slaveIDs;   // 在线数据迁移 虚拟从节点
    private boolean tempReadHostAvailable = false;  //如果写服务挂掉, 临时读服务是否继续可用
    
    private MySQLMetaBean writeMetaBean;
    private List<MySQLMetaBean> readMetaBeans = new ArrayList<>();

    private int writeIndex = 0; //主节点默认为0
	
    public void initMaster() {
        // 根据配置replica-index的配置文件修改主节点
        MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        Integer repIndex = conf.getRepIndex(name);
        if (repIndex != null) {
            writeIndex = repIndex;
        }
        writeMetaBean = mysqls.get(writeIndex);
        writeMetaBean.setSlaveNode(false);
        readMetaBeans.addAll(mysqls);
        readMetaBeans.remove(writeIndex);
    }
    
	public void doHeartbeat() {

		if (writeMetaBean == null) {
			return;
		}

		for (MySQLMetaBean source : this.mysqls) {

			if (source != null) {
				source.doHeartbeat();
			} else {
				StringBuilder s = new StringBuilder();
				s.append(Alarms.DEFAULT).append(name).append(" current dataSource is null!");
				logger.error(s.toString());
			}
		}

	}
    

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RepTypeEnum getType() {
        return type;
    }

    public void setType(RepTypeEnum type) {
        this.type = type;
    }

    public RepSwitchTypeEnum getSwitchType() {
        return switchType;
    }

    public void setSwitchType(RepSwitchTypeEnum switchType) {
        this.switchType = switchType;
    }

    public List<MySQLMetaBean> getMysqls() {
        return mysqls;
    }

    public void setMysqls(List<MySQLMetaBean> mysqls) {
        this.mysqls = mysqls;
    }

    /**
     * 得到当前用于写的MySQLMetaBean
     */
    private MySQLMetaBean getCurWriteMetaBean() {
        return writeMetaBean.isAlive()?writeMetaBean:null;
    }
    
    
    public MySQLMetaBean getBalanceMetaBean(boolean runOnSlave){
    	
    	if(!runOnSlave){
    		return getCurWriteMetaBean();
    	}
    	
    	MySQLMetaBean datas = null;
    	
		switch(balance){
			case BALANCE_ALL:
				datas = getLBReadWriteMetaBean();
				break;
			case BALANCE_ALL_READ:
				datas = getLBReadMetaBean();
				//如果从节点不可用,从主节点获取连接
				if(datas==null){
					logger.debug("all slaveNode is Unavailable. use master node for read . balance type is {}",balance);
					datas = getCurWriteMetaBean();
				}
				break;
			case BALANCE_NONE:
				datas = getCurWriteMetaBean();
				break;
			default:
				logger.debug("current balancetype is not supported!! [{}], use writenode connection .",balance);
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
    	List<MySQLMetaBean> result = mysqls.stream()
    			.filter(f->f.canSelectAsReadNode())
    			.collect(Collectors.toList());
        return result.isEmpty()?null:result.get(ThreadLocalRandom.current().nextInt(result.size()));
    }

    /**
     * 只有当前读节点 承担负载
     * @return
     */
    private MySQLMetaBean getLBReadMetaBean(){
    	List<MySQLMetaBean> result = readMetaBeans.stream()
    			.filter(f->f.canSelectAsReadNode())
    			.collect(Collectors.toList());
    	return result.isEmpty()?null:result.get(ThreadLocalRandom.current().nextInt(result.size()));
    }

    @Override
    public String toString() {
        return "MySQLRepBean [name=" + name + ", type=" + type + ", switchType=" + switchType + ", mysqls=" + mysqls + "]";
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

	public void setWriteMetaBean(MySQLMetaBean writeMetaBean) {
		this.writeMetaBean = writeMetaBean;
	}

	public void setReadMetaBeans(List<MySQLMetaBean> readMetaBeans) {
		this.readMetaBeans = readMetaBeans;
	}


	public BalanceTypeEnum getBalance() {
		return balance;
	}


	public void setBalance(BalanceTypeEnum balance) {
		this.balance = balance;
	}
}
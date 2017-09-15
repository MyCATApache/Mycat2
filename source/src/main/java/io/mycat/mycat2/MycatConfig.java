package io.mycat.mycat2;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.proxy.ProxyConfig;

public class MycatConfig extends ProxyConfig {
	
	public static final long DEFAULT_IDLE_TIMEOUT 				= 30 * 60 * 1000L;
	public static final long DEFAULT_REPLICA_IDLE_CHECK_PERIOD  = 5 * 60 * 1000L;
	public static final long DEFAULT_REPLICA_HEARTBEAT_PERIOD   = 10 * 1000L;
	public static final int  DEFAULT_TIMEREXECTOR               = 2;
	public static final long DEFAULT_PROCESSOR_CHECK_PERIOD     = 1 * 1000L;
	
	
	// 默认空闲超时时间
	private long idleTimeout;
	// 默认复制组 空闲检查周期
	private long replicaIdleCheckPeriod;
	// 默认复制组心跳周期 
	private long replicaHeartbeatPeriod;
	
	private int timerExecutor = 0;
	
	// sql execute timeout (second)
	private long sqlExecuteTimeout = 300;
	private long processorCheckPeriod;
	
	private long minSwitchtimeInterval = 30 * 60 * 1000L;  //默认三十分钟

	/**
	 * 系统中所有MySQLRepBean的Map
	 */
	private Map<String, MySQLRepBean> mysqlRepMap = new HashMap<String, MySQLRepBean>();

	/**
	 * 系统中所有SchemaBean的Map
	 */
	private Map<String, SchemaBean> mycatSchemaMap = new HashMap<String, SchemaBean>();

	/**
	 * 默认Schema,取配置文件种第一个Schema
	 */
	private SchemaBean defaultSchemaBean;

	private Map<String, Integer> repIndexMap = new HashMap<String, Integer>();

	public Map<String, MySQLRepBean> getMysqlRepMap() {
		return this.mysqlRepMap;
	}

	protected void addMySQLRepBean(final MySQLRepBean mySQLRepBean) {
		this.mysqlRepMap.put(mySQLRepBean.getName(), mySQLRepBean);
	}

	protected void addSchemaBean(SchemaBean schemaBean) {
		if (defaultSchemaBean == null) {
			defaultSchemaBean = schemaBean;
		}
		this.mycatSchemaMap.put(schemaBean.getName(), schemaBean);
	}

	public SchemaBean getMycatSchema(String schema) {
		return this.mycatSchemaMap.get(schema);
	}

	public SchemaBean getDefaultMycatSchema() {
		return this.defaultSchemaBean;
	}

	public MySQLRepBean getMySQLRepBean(String repName) {
		return this.mysqlRepMap.get(repName);
	}
	
	public Collection<MySQLRepBean> getMySQLReplicaSet(){
		return this.mysqlRepMap.values();
	}

	public Integer getRepIndex(String repName) {
		return repIndexMap.get(repName);
	}

	public void addRepIndex(ReplicaIndexBean replicaIndexBean) {
		if (replicaIndexBean != null && replicaIndexBean.getReplicaIndexes() != null) {
			replicaIndexBean.getReplicaIndexes().forEach((key, value) -> repIndexMap.put(key, value));
		}
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public int getTimerExecutor() {
		return timerExecutor;
	}

	public void setTimerExecutor(int timerExecutor) {
		this.timerExecutor = timerExecutor;
	}

	public long getSqlExecuteTimeout() {
		return sqlExecuteTimeout;
	}

	public void setSqlExecuteTimeout(long sqlExecuteTimeout) {
		this.sqlExecuteTimeout = sqlExecuteTimeout;
	}

	public long getProcessorCheckPeriod() {
		return processorCheckPeriod;
	}

	public void setProcessorCheckPeriod(long processorCheckPeriod) {
		this.processorCheckPeriod = processorCheckPeriod;
	}

	public long getReplicaIdleCheckPeriod() {
		return replicaIdleCheckPeriod;
	}

	public void setReplicaIdleCheckPeriod(long replicaIdleCheckPeriod) {
		this.replicaIdleCheckPeriod = replicaIdleCheckPeriod;
	}

	public long getReplicaHeartbeatPeriod() {
		return replicaHeartbeatPeriod;
	}

	public void setReplicaHeartbeatPeriod(long replicaHeartbeatPeriod) {
		this.replicaHeartbeatPeriod = replicaHeartbeatPeriod;
	}

	public long getMinSwitchtimeInterval() {
		return minSwitchtimeInterval;
	}

	public void setMinSwitchtimeInterval(long minSwitchtimeInterval) {
		this.minSwitchtimeInterval = minSwitchtimeInterval;
	}
}

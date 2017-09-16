package io.mycat.mycat2;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.Configurable;
import io.mycat.proxy.ProxyConfig;

public class MycatConfig extends ProxyConfig {
	// 当前节点所用的配置文件的版本
	private Map<Byte, Integer> configVersionMap = new HashMap<>();
	private Map<Byte, Configurable> configMap = new HashMap<>();

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

	private Map<String, Integer> repIndexMap;

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

	public Map<String, Integer> getRepIndexMap() {
		return this.repIndexMap;
	}

	public void addRepIndex(ReplicaIndexBean replicaIndexBean) {
		if (replicaIndexBean != null && replicaIndexBean.getReplicaIndexes() != null) {
			repIndexMap = replicaIndexBean.getReplicaIndexes();
		}
	}

	public int getConfigVersion(byte configKey) {
		Integer oldVersion = configVersionMap.get(configKey);
		return oldVersion == null ? ConfigEnum.INIT_VERSION : oldVersion;
	}

	public int getNextConfigVersion(byte configKey) {
		return getConfigVersion(configKey) + 1;
	}

	public Configurable getConfig(byte configKey) {
		return configMap.get(configKey);
	}

	public void putConfig(byte configKey, Configurable config, Integer version) {
		configMap.put(configKey, config);
		version = version == null ? ConfigEnum.INIT_VERSION : version;
		configVersionMap.put(configKey, version);
	}

	public Map<Byte, Integer> getConfigVersionMap() {
		return configVersionMap;
	}
}

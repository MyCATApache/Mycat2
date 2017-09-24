package io.mycat.mycat2;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.beans.GlobalBean;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.conf.ProxyConfig;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.Configurable;

public class MycatConfig {
	private ProxyConfig proxyConfig;

	// 当前节点所用的配置文件的版本
	private Map<ConfigEnum, Integer> configVersionMap = new HashMap<>();
	private Map<ConfigEnum, Configurable> configMap = new HashMap<>();

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

	public MycatConfig(ProxyConfig proxyConfig) {
		this.proxyConfig = proxyConfig;
	}

	public Map<String, MySQLRepBean> getMysqlRepMap() {
		return this.mysqlRepMap;
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

	public void setConfigVersion(ConfigEnum configEnum, int version) {
		configVersionMap.put(configEnum, version);
	}

	public int getConfigVersion(ConfigEnum configEnum) {
		Integer oldVersion = configVersionMap.get(configEnum);
		return oldVersion == null ? GlobalBean.INIT_VERSION : oldVersion;
	}

	public int getNextConfigVersion(ConfigEnum configEnum) {
		return getConfigVersion(configEnum) + 1;
	}

	public Configurable getConfig(ConfigEnum configEnum) {
		return configMap.get(configEnum);
	}

	public void putConfig(ConfigEnum configEnum, Configurable config, Integer version) {
		configMap.put(configEnum, config);
		version = version == null ? GlobalBean.INIT_VERSION : version;
		configVersionMap.put(configEnum, version);
	}

	public Map<ConfigEnum, Integer> getConfigVersionMap() {
		return configVersionMap;
	}
}

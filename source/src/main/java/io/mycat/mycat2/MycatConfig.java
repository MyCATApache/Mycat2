package io.mycat.mycat2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.proxy.ProxyConfig;

public class MycatConfig extends ProxyConfig {

	/**
	 * 系统中所有MySQLRepBean的Map
	 */
	private Map<String, MySQLRepBean> msqlRepMap = new HashMap<String, MySQLRepBean>();

	/**
	 * 系统中所有SchemaBean的Map
	 */
	private Map<String, SchemaBean> mycatSchemaMap = new HashMap<String, SchemaBean>();

	/**
	 * 默认Schema,取配置文件种第一个Schema
	 */
	private SchemaBean defaultSchemaBean;

	private Map<String, Integer> repIndexMap = new HashMap<String, Integer>();

	protected void addMySQLRepBean(final MySQLRepBean mySQLRepBean) {
		this.msqlRepMap.put(mySQLRepBean.getName(), mySQLRepBean);
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
		return this.msqlRepMap.get(repName);
	}

	public Integer getRepIndex(String repName) {
		return repIndexMap.get(repName);
	}

	public void addRepIndex(ReplicaIndexBean replicaIndexBean) {
		if (replicaIndexBean != null && replicaIndexBean.getReplicaIndexes() != null) {
			replicaIndexBean.getReplicaIndexes().forEach((key, value) -> repIndexMap.put(key, value));
		}
	}
}

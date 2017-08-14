package io.mycat.mycat2;

import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.proxy.ProxyConfig;

public class MycatConfig extends ProxyConfig {

	/**
	 * 系统中所有MySQLReplicatSet的Map
	 */
	private Map<String, MySQLReplicatSet> msqlRepSetMap = new HashMap<String, MySQLReplicatSet>();

	/**
	 * 系统中所有SchemaBean的Map
	 */
	private Map<String, SchemaBean> mycatSchemaMap = new HashMap<String, SchemaBean>();

	/**
	 * 默认Schema,取配置文件种第一个Schema
	 */
	private SchemaBean defaultSchemaBean;



	protected void addMySQLReplicatSet(final MySQLReplicatSet repSet) {
		final String repSetName = repSet.getName();
		this.msqlRepSetMap.put(repSetName, repSet);
	}

	protected void addSchemaBean(SchemaBean schemaBean) {
		if (defaultSchemaBean == null) { // call by MycatCore,在配置文件加载时初始化
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

	public MySQLReplicatSet getMySQLReplicatSet(String repsetName) {
		return this.msqlRepSetMap.get(repsetName);
	}

}

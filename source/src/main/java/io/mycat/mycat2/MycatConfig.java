package io.mycat.mycat2;

import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.beans.GlobalBean;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.conf.DatasourceConfig;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.beans.conf.SchemaConfig;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.Configurable;

public class MycatConfig {
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

    public void initRepMap() {
        DatasourceConfig dsConfig = getConfig(ConfigEnum.DATASOURCE);
        dsConfig.getReplicas().forEach(replica -> {
            MySQLRepBean repBean = new MySQLRepBean();
            repBean.setReplicaBean(replica);
            mysqlRepMap.put(replica.getName(), repBean);
        });
    }

    public void initSchemaMap() {
        SchemaConfig schemaConfig = getConfig(ConfigEnum.SCHEMA);
        schemaConfig.getSchemas().forEach(schema -> {
            if (defaultSchemaBean == null) {
                defaultSchemaBean = schema;
            }
            mycatSchemaMap.put(schema.getName(), schema);
        });
    }

    public MySQLRepBean getMySQLRepBean(String repName) {
        return mysqlRepMap.get(repName);
    }

    public SchemaBean getSchemaBean(String schemaName) {
        return mycatSchemaMap.get(schemaName);
    }

    /**
     * 获取指定的配置对象
     */
	public <T> T getConfig(ConfigEnum configEnum) {
		return (T) configMap.get(configEnum);
	}

    /**
     * 添加配置对象,指定版本号,默认版本为1
     * @param configEnum
     * @param config
     * @param version
     */
	public void putConfig(ConfigEnum configEnum, Configurable config, int version) {
		configMap.put(configEnum, config);
		configVersionMap.put(configEnum, version);
	}

	public Map<ConfigEnum, Integer> getConfigVersionMap() {
		return configVersionMap;
	}

	public void setConfigVersion(ConfigEnum configEnum, int version) {
		configVersionMap.put(configEnum, version);
	}

	public int getConfigVersion(ConfigEnum configEnum) {
		return configVersionMap.get(configEnum);
	}

    public Map<String, MySQLRepBean> getMysqlRepMap() {
        return mysqlRepMap;
    }

    public SchemaBean getDefaultSchemaBean() {
        return defaultSchemaBean;
    }
}

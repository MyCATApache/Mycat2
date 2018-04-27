package io.mycat.mycat2;

import io.mycat.mycat2.beans.GlobalBean;
import io.mycat.mycat2.beans.MySQLRepBean;
import io.mycat.mycat2.beans.conf.*;
import io.mycat.mycat2.sqlparser.MatchMethodGenerator;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.Configurable;
import io.mycat.util.SplitUtil;

import java.util.HashMap;
import java.util.Map;

public class MycatConfig {
	// 当前节点所用的配置文件的版本
	private Map<ConfigEnum, Integer> configVersionMap = new HashMap<>();
	private Map<ConfigEnum, Configurable> configMap = new HashMap<>();
	private Map<ConfigEnum, Long> configUpdateTimeMap = new HashMap<>();

    /**
     * 系统中所有MySQLRepBean的Map
     */
    private Map<String, MySQLRepBean> mysqlRepMap = new HashMap<String, MySQLRepBean>();
    /**
     * 系统中所有SchemaBean的Map
     */
    private Map<String, SchemaBean> mycatSchemaMap = new HashMap<String, SchemaBean>();
    /**
     * 系统中所有DataNode的Map
     */
    private Map<String, DNBean> mycatDataNodeMap = new HashMap<>();

    private Map<Long, DNBean> mycatLong2DataNodeMap = new HashMap<>();
    /**
     * 系统中所有TableDefBean的Map
     */
    private Map<String, TableDefBean> mycatTableMap = new HashMap<String, TableDefBean>();
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
        schemaConfig.getDataNodes().forEach(dataNode -> {
            mycatDataNodeMap.put(dataNode.getName(), dataNode);
            mycatLong2DataNodeMap.put(MatchMethodGenerator.genHash(dataNode.getName().toCharArray()), dataNode);
        });
        schemaConfig.getSchemas().forEach(schema -> {
            if (defaultSchemaBean == null) {
                defaultSchemaBean = schema;
            }
            mycatSchemaMap.put(schema.getName(), schema);
            schema.getTables().forEach(table -> {
                String theDataNodes[] = SplitUtil.split(table.getDataNode(), ',', '$', '-');
                if (theDataNodes == null || theDataNodes.length <= 0) {
                    throw new IllegalArgumentException(
                            "invalid table dataNodes: " + table.getDataNode());
                }
                for (String dn : theDataNodes) {
                    table.getDataNodes().add(dn);
                }
                mycatTableMap.put(table.getName(), table);
            });
        });


    }

    public MySQLRepBean getMySQLRepBean(String repName) {
        return mysqlRepMap.get(repName);
    }

    public SchemaBean getSchemaBean(String schemaName) {
        return mycatSchemaMap.get(schemaName);
    }

    public TableDefBean getTableDefBean(String tableName) {
        return mycatTableMap.get(tableName);
    }

    public DNBean getDNBean(String dataNodeName) {
        return mycatDataNodeMap.get(dataNodeName);
    }

    public DNBean getDNBean(long dataNodeName) {
        return mycatLong2DataNodeMap.get(dataNodeName);
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
        configUpdateTimeMap.put(configEnum, System.currentTimeMillis());
	}

	public Map<ConfigEnum, Integer> getConfigVersionMap() {
		return configVersionMap;
	}

	public void setConfigVersion(ConfigEnum configEnum, int version) {
		configVersionMap.put(configEnum, version);
        configUpdateTimeMap.put(configEnum, System.currentTimeMillis());
	}

	public int getConfigVersion(ConfigEnum configEnum) {
        Integer oldVersion = configVersionMap.get(configEnum);
        return oldVersion == null ? GlobalBean.INIT_VERSION : oldVersion;
	}

	public long getConfigUpdateTime(ConfigEnum configEnum) {
	    return configUpdateTimeMap.get(configEnum);
    }

    public Map<String, MySQLRepBean> getMysqlRepMap() {
        return mysqlRepMap;
    }

    public Map<String, DNBean> getMycatDataNodeMap() {
        return mycatDataNodeMap;
    }

    public SchemaBean getDefaultSchemaBean() {
        return defaultSchemaBean;
    }

	public Map<String, SchemaBean> getMycatSchemaMap() {
		return mycatSchemaMap;
	}

	public void setMycatSchemaMap(Map<String, SchemaBean> mycatSchemaMap) {
		this.mycatSchemaMap = mycatSchemaMap;
	}
    
    
}

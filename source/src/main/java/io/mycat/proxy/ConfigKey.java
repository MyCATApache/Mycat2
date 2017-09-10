package io.mycat.proxy;

/**
 * Desc: 用于指定集群配置文件的版本map中的key
 *
 * @date: 06/09/2017
 * @author: gaozhiwen
 */
public class ConfigKey {
    // 对应mycat.conf
    public static final String MYCAT_CONF = "mycatConf";

    // 对应datasource.xml
    public static final String DATASOURCE= "datasource";

    // 对应replica-index.properties
    public static final String REPLICA_INDEX = "replicaIndex";

    // 对应scheschema.xmls
    public static final String SCHEMA = "schema";

    // 对应sharding-rule.xml
    public static final String SHARDING_RULE = "shardingRule";

    public static int INIT_VERSION = 1;
}

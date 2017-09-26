package io.mycat.proxy;

import io.mycat.mycat2.beans.conf.*;

import java.util.stream.Stream;

/**
 * Desc: 用于指定集群配置文件的枚举值
 *
 * @date: 06/09/2017
 * @author: gaozhiwen
 */
public enum ConfigEnum {
    PROXY((byte) 1, "mycat.yml", ProxyConfig.class),
    CLUSTER((byte) 2, "cluster.yml", ClusterConfig.class),
    BALANCER((byte) 3, "balancer.yml", BalancerConfig.class),
    HEARTBEAT((byte) 4, "heartbeat.yml", HeartbeatConfig.class),
    DATASOURCE((byte) 5, "datasource.yml", DatasourceConfig.class),
    REPLICA_INDEX((byte) 6, "replica-index.yml", ReplicaIndexConfig.class),
    SCHEMA((byte) 7, "schema.yml", SchemaConfig.class),
    SHARDING_RULE((byte) 8, "sharding-rule.yml", ShardingRuleConfig.class);

    private byte type;
    private String fileName;
    private Class clazz;

    ConfigEnum(byte type, String fileName, Class clazz) {
        this.type = type;
        this.fileName = fileName;
        this.clazz = clazz;
    }

    public byte getType() {
        return this.type;
    }

    public String getFileName() {
        return this.fileName;
    }

    public Class getClazz() {
        return clazz;
    }

    public static ConfigEnum getConfigEnum(byte type) {
        return Stream.of(ConfigEnum.values()).filter(configEnum -> configEnum.type == type)
                .findFirst().orElse(null);
    }
}

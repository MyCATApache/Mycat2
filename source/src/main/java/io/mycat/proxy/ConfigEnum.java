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
    PROXY(1, "mycat.yml", ProxyConfig.class),
    CLUSTER(2, "cluster.yml", ClusterConfig.class),
    BALANCER(3, "balancer.yml", BalancerConfig.class),
    HEARTBEAT(4, "heartbeat.yml", HeartbeatConfig.class),
    DATASOURCE(5, "datasource.yml", DatasourceConfig.class),
    REPLICA_INDEX(6, "replica-index.yml", ReplicaIndexConfig.class),
    SCHEMA(7, "schema.yml", SchemaConfig.class),
    SHARDING_RULE(8, "sharding-rule.yml", ShardingRuleConfig.class);

    private byte type;
    private String fileName;
    private Class clazz;

    ConfigEnum(int type, String fileName, Class clazz) {
        this.type = (byte) type;
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

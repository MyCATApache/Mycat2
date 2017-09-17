package io.mycat.proxy;

import io.mycat.mycat2.beans.ReplicaConfBean;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.mycat2.beans.SchemaConfBean;
import io.mycat.mycat2.beans.ShardingRuleBean;

import java.util.stream.Stream;

/**
 * Desc: 用于指定集群配置文件的枚举值
 *
 * @date: 06/09/2017
 * @author: gaozhiwen
 */
public enum ConfigEnum {
    DATASOURCE((byte) 1, "datasource.yml", ReplicaConfBean.class),
    REPLICA_INDEX((byte) 2, "replica-index.yml", ReplicaIndexBean.class),
    SCHEMA((byte) 3, "schema.yml", SchemaConfBean.class),
    SHARDING_RULE((byte) 4, "sharding-rule.yml", ShardingRuleBean.class);

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

    public static final int INIT_VERSION = 1;
}

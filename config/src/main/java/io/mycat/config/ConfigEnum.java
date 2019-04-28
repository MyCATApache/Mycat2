package io.mycat.config;


import io.mycat.config.datasource.DatasourceRootConfig;
import io.mycat.config.datasource.ReplicaIndexRootConfig;
import io.mycat.config.proxy.ProxyRootConfig;
import io.mycat.config.schema.SchemaRootConfig;
import io.mycat.config.user.UserRootConfig;

/**
 * Desc: 用于指定集群配置文件的枚举值
 *
 * @date: 06/09/2017,04/09/2019
 * @author: gaozhiwen chenjunwen
 */
public enum ConfigEnum {
    PROXY(1, "mycat.yml", ProxyRootConfig.class),
    USER(5, "user.yml", UserRootConfig.class),
    DATASOURCE(6, "datasource.yml", DatasourceRootConfig.class),
    REPLICA_INDEX(7, "replica-index.yml", ReplicaIndexRootConfig.class),
    SCHEMA(8, "schema.yml", SchemaRootConfig.class);
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
        ConfigEnum[] values = ConfigEnum.values();
        int length = values.length;
        for (int i = 0; i < length; i++) {
            ConfigEnum value = values[i];
            if (values[i].getType() == type) {
                return value;
            }
        }
        throw new RuntimeException("illegal type:" + type);
    }
}

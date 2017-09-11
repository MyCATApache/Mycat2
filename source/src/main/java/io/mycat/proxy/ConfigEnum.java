package io.mycat.proxy;

import java.util.stream.Stream;

/**
 * Desc: 用于指定集群配置文件的枚举值
 *
 * @date: 06/09/2017
 * @author: gaozhiwen
 */
public enum ConfigEnum {
    DATASOURCE((byte) 1, "datasource.yml"),
    REPLICA_INDEX((byte) 2, "replica-index.yml"),
    SCHEMA((byte) 3, "schema.yml"),
    SHARDING_RULE((byte) 4, "sharding-rule.yml");

    private byte code;
    private String fileName;

    ConfigEnum(byte code, String fileName) {
        this.code = code;
        this.fileName = fileName;
    }

    public byte getCode() {
        return this.code;
    }

    public String getFileName() {
        return this.fileName;
    }

    public static ConfigEnum getConfigEnum(byte code) {
        return Stream.of(ConfigEnum.values()).filter(configEnum -> configEnum.code == code)
                .findFirst().orElse(null);
    }

    public static final int INIT_VERSION = 1;
}

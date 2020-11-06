package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class LoadBalanceConfig {
    String name;
    String clazz;

    public LoadBalanceConfig() {
    }

    public LoadBalanceConfig(String name, String clazz) {
        this.name = name;
        this.clazz = clazz;
    }
}
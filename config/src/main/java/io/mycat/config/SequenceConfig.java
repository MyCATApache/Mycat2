package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class SequenceConfig {
    String name;
    String clazz;
    String args;

    public SequenceConfig(String name, String clazz, String args) {
        this.name = name;
        this.clazz = clazz;
        this.args = args;
    }

    public SequenceConfig() {
    }
}
package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@Data
@EqualsAndHashCode
public class SequenceConfig {
    String uniqueName;
    String clazz;
    Map<String, Object> args;

    public SequenceConfig( String uniqueName, String clazz, Map<String, Object> args) {
        this.uniqueName = uniqueName;
        this.clazz = clazz;
        this.args = args;
    }

    public SequenceConfig() {
    }
}
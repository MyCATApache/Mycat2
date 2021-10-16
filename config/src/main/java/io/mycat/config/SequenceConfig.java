package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class SequenceConfig implements KVObject{
    private String name;

    // ---------给用户自行扩展预留字段------------

    private String startWith;
    private String incrementBy;
    private String minValue;
    private String maxValue;
    private boolean noMaxValue;
    private boolean noMinValue;
    private Boolean withCache;
    private Boolean cycle;
    private Boolean cache;
    private Long cacheValue;
    private Boolean order;

    private boolean simple;
    private boolean group;
    private boolean time;

    private Long unitCount;
    private Long unitIndex;

    private Long step;

    // ---------给用户自定义预留------------

    private String clazz;

    private String targetName;
    private String schemaName;
    private String fileName;

    public String getName() {
        return name.toLowerCase();
    }

    public void setName(String name) {
        this.name = name.toLowerCase();
    }

    public SequenceConfig() {
    }

    @Override
    public String keyName() {
        return name;
    }

    @Override
    public String path() {
        return "sequences";
    }

    @Override
    public String fileName() {
        return "sequence";
    }
}
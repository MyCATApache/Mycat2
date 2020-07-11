package io.mycat.hbt4;

import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode
public class ShardingInfo {
    final List<String> schemaKeys;
    final List<String> tableKeys;
    final int size;
    final String name;

    public ShardingInfo(List<String> schemaKeys, List<String> tableKeys, int size, String name) {
        this.schemaKeys = schemaKeys;
        this.tableKeys = tableKeys;
        this.size = size;
        this.name = name;
    }

    public List<String> getSchemaKeys() {
        return schemaKeys;
    }

    public List<String> getTableKeys() {
        return tableKeys;
    }

    public int getSize() {
        return size;
    }

    public String getName() {
        return name;
    }

    public boolean isGlobal() {
        return size == 1&&schemaKeys.isEmpty()&&tableKeys.isEmpty();
    }
}
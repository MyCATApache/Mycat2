package io.mycat.hbt4;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

@EqualsAndHashCode
@Getter
public class ShardingInfo {
    final List<String> schemaKeys;
    final String schemaFun;
    final List<String> tableKeys;
    final String tableFun;
    final int datasourceSize;
    final int schemaSize;
    final int tableSize;
    final String name;
    final Type type;

    public ShardingInfo(Type type,
                        List<String> schemaKeys, List<String> tableKeys,
                        String schemaFun, String tableFun,
                        int datasourceSize,
                        int schemaSize, int tableSize,
                        String name) {
        this.type = type;
        this.schemaKeys = schemaKeys;
        this.tableKeys = tableKeys;
        this.schemaFun = schemaFun;
        this.tableFun = tableFun;
        this.datasourceSize = datasourceSize;
        this.schemaSize = schemaSize;
        this.tableSize = tableSize;
        this.name = name;
    }

    public int size(){
        return datasourceSize*schemaSize*tableSize;
    }

    public boolean isBroadCast() {
     return type == Type.broadCast;
    }
    public boolean isNormal() {
        return type == Type.normal;
    }
    public boolean isSharding() {
        return type == Type.sharding;
    }
    public enum Type{
        broadCast,
        normal,
        sharding
    }
}
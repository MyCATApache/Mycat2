/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
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
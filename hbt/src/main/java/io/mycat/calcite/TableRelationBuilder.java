///**
// * Copyright (C) <2020>  <chen junwen>
// * <p>
// * This program is free software: you can redistribute it and/or modify it under the terms of the
// * GNU General Public License as published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// * General Public License for more details.
// * <p>
// * You should have received a copy of the GNU General Public License along with this program.  If
// * not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat.calcite;
//
//import lombok.AllArgsConstructor;
//import lombok.EqualsAndHashCode;
//
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Set;
//
//public class TableRelationBuilder {
//    HashMap<ShardingInfo, Set<SchemaInfo>> builder = new HashMap<>();
//
//    public void add(String schemaName, String tableName, ShardingInfo shardingInfo) {
//        Set<SchemaInfo> set = builder.computeIfAbsent(shardingInfo, shardingInfo1 -> new HashSet<>());
//        set.add(new SchemaInfo(schemaName, tableName));
//    }
//
//    public TableRelationChecker build() {
//        return (shardingInfo, schemaName, tableName) -> {
//            Set<SchemaInfo> schemaInfos = builder.get(shardingInfo);
//            if (schemaInfos == null) {
//                return false;
//            }
//            return schemaInfos.contains(new SchemaInfo(schemaName, tableName));
//        };
//    }
//
//    @AllArgsConstructor
//    @EqualsAndHashCode
//    public static class SchemaInfo {
//        final String targetSchema;
//        final String targetTable;
//    }
//}
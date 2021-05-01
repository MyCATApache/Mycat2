///**
// * Copyright (C) <2021>  <chen junwen>
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
//package io.mycat.queryCondition;
//
//import io.mycat.BackendTableInfo;
//import io.mycat.SchemaInfo;
//import io.mycat.calcite.table.ShardingTable;
//import io.mycat.router.RuleFunction;
//import lombok.NonNull;
//
//import java.util.*;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
///**
// * @author Junwen Chen
// **/
//public class DataMappingEvaluatorImpl {
//    private final Map<String, HashSet<RangeVariable>> columnMap = new HashMap<>();
//
//    public  void assignment(boolean or, String columnName, String value) {
//        getRangeVariables(columnName).add(new RangeVariable(or, RangeVariableType.EQUAL, value));
//    }
//
//    public    void assignmentRange(boolean or, String columnName, String begin, String end) {
//        getRangeVariables(columnName).add(new RangeVariable(or, RangeVariableType.RANGE, begin, end));
//    }
//
//    private Set<RangeVariable> getRangeVariables(String columnName) {
//        return columnMap.computeIfAbsent(columnName, s -> new HashSet<>());
//    }
//
//    public List<BackendTableInfo> calculate(ShardingTable logicTable) {
//        if (logicTable.getNatureTableColumnInfo() != null) {
//            return getBackendTableInfosByNatureDatabaseTable(logicTable).stream().map(integer -> logicTable.getBackends().get(integer)).collect(Collectors.toList());
//        } else {
//            return getBackendTableInfosByMap(logicTable);
//        }
//    }
//
//    private List<BackendTableInfo> getBackendTableInfosByMap(ShardingTable logicTable) {
//        List<String> targetSet = Collections.emptyList();
//        List<String> databaseSet = Collections.emptyList();
//        List<String> tableSet = Collections.emptyList();
//        if (logicTable.getReplicaColumnInfo() != null) {
//            targetSet = getRouteColumnSortedSet(logicTable.getReplicaColumnInfo());
//        }
//        if (logicTable.getDatabaseColumnInfo() != null) {
//            databaseSet = getRouteColumnSortedSet(logicTable.getDatabaseColumnInfo());
//        }
//        if (logicTable.getTableColumnInfo() != null) {
//            tableSet = getRouteColumnSortedSet(logicTable.getTableColumnInfo());
//        }
//        List<BackendTableInfo> res = new ArrayList<>();
//
//        @NonNull List<BackendTableInfo> allBackends = logicTable.getBackends();
//
//        for (String targetName : targetSet) {
//            for (String databaseName : databaseSet) {
//                for (String tableName : tableSet) {
//                    res.add(new BackendTableInfo(targetName, new SchemaInfo(databaseName, tableName)));
//                }
//            }
//        }
//        if (res.isEmpty()) {
//            return allBackends;
//        } else {
//            if (allBackends.isEmpty()) {
//                return res;
//            }
//            return res.stream().filter(allBackends::contains).collect(Collectors.toList());
//        }
//    }
//
//    private List<Integer> getBackendTableInfosByNatureDatabaseTable(ShardingTable logicTable) {
//        List<Integer> routeIndexSortedSet = Collections.emptyList();
//        if (!columnMap.isEmpty()) {
//            routeIndexSortedSet = getRouteIndexSortedSet(logicTable.getNatureTableColumnInfo());
//        }
//        if (routeIndexSortedSet.isEmpty()) {
//            return IntStream.range(0,logicTable.getBackends().size()).boxed().collect(Collectors.toList());
//        } else {
//            return routeIndexSortedSet;
//        }
//    }
//
//    private List<String> getRouteColumnSortedSet(SimpleColumnInfo.ShardingInfo target) {
//        return getRouteIndexSortedSet(target).stream().map(i -> target.map.get(i)).collect(Collectors.toList());
//    }
//
//    private List<Integer> getRouteIndexSortedSet(SimpleColumnInfo.ShardingInfo target) {
//        @NonNull SimpleColumnInfo columnInfo = target.columnInfo;
//        Set<RangeVariable> rangeVariables = columnMap.get(columnInfo.columnName);
//        if (rangeVariables == null) {
//            return IntStream.range(0, target.map.size()).boxed().collect(Collectors.toList());
//        } else {
//            return calculate(target.getFunction(), rangeVariables).stream().sorted().collect(Collectors.toList());
//        }
//    }
//
//    private Set<Integer> calculate(RuleFunction ruleFunction, Set<RangeVariable> value) {
//        HashSet<Integer> res = new HashSet<>();
//        for (RangeVariable rangeVariable : value) {
//            String begin = Objects.toString(rangeVariable.getBegin());
//            String end = Objects.toString(rangeVariable.getEnd());
//            switch (rangeVariable.getOperator()) {
//                case EQUAL: {
//                    int calculate = ruleFunction.calculate(begin);
//                    if (calculate == -1) {
//                        return Collections.emptySet();
//                    }
//                    res.add(calculate);
//                    break;
//                }
//                case RANGE: {
//                    int[] calculate = ruleFunction.calculateRange(begin, end);
//                    if (calculate == null || calculate.length == 0) {
//                        return Collections.emptySet();
//                    }
//                    for (int i : calculate) {
//                        if (i == -1) {
//                            return Collections.emptySet();
//                        }
//                        res.add(i);
//                    }
//                    break;
//                }
//            }
//        }
//        return res;
//    }
////
////    public String getFilterExpr(List<String> rowOrder) {
////        StringBuilder where = new StringBuilder();
////        for (String columnName : rowOrder) {
////            SortedSet<RangeVariable> value = columnMap.get(columnName);
////            for (RangeVariable rangeVariable : value) {
////                if (where.length() > 0) {
////                    if (rangeVariable.isOr()) {
////                        where.append(" or (");
////                    } else {
////                        where.append(" and (");
////                    }
////                } else {
////                    where.append("  (");
////                }
////                switch (rangeVariable.getOperator()) {
////                    case EQUAL:
////                        where.append(columnName).append(" = ").append(rangeVariable.getBegin());
////                        break;
////                    case RANGE:
////                        where.append(columnName).append(" between ").append(rangeVariable.getBegin()).append(" and ").append(rangeVariable.getEnd());
////                        break;
////                }
////                where.append(" ) ");
////            }
////        }
////        return where.toString();
////    }
//
//    public void merge(DataMappingEvaluatorImpl arg) {
//        arg.columnMap.forEach((key, value) -> this.getRangeVariables(key).addAll(value));
//    }
//}
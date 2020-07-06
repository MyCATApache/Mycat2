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
package io.mycat.querycondition;

import io.mycat.*;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import lombok.NonNull;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public class DataMappingEvaluator {
    private final Map<String, HashSet<RangeVariable>> columnMap = new HashMap<>();

    public void assignment(boolean or, String columnName, String value) {
        getRangeVariables(columnName).add(new RangeVariable(columnName, or, RangeVariableType.EQUAL, value));
    }

    public void assignmentRange(boolean or, String columnName, String begin, String end) {
        getRangeVariables(columnName).add(new RangeVariable(columnName, or, RangeVariableType.RANGE, begin, end));
    }

    private Set<RangeVariable> getRangeVariables(String columnName) {
        return columnMap.computeIfAbsent(columnName, s -> new HashSet<>());
    }

    public List<DataNode> calculate(ShardingTableHandler logicTable) {
        if (logicTable.isNatureTable()) {
            return getBackendTableInfosByNatureDatabaseTable(logicTable);
        } else {
            return getBackendTableInfosByMap(logicTable);
        }
    }

    private List<DataNode> getBackendTableInfosByMap(ShardingTableHandler logicTable) {
        List<String> targetSet = Collections.emptyList();
        List<String> databaseSet = Collections.emptyList();
        List<String> tableSet = Collections.emptyList();
        if (logicTable.getReplicaColumnInfo() != null) {
            targetSet = getRouteColumnSortedSet(logicTable.getReplicaColumnInfo());
        }
        if (logicTable.getDatabaseColumnInfo() != null) {
            databaseSet = getRouteColumnSortedSet(logicTable.getDatabaseColumnInfo());
        }
        if (logicTable.getTableColumnInfo() != null) {
            tableSet = getRouteColumnSortedSet(logicTable.getTableColumnInfo());
        }
        List<BackendTableInfo> res = new ArrayList<>();

        @NonNull List<DataNode> allBackends = logicTable.getShardingBackends();

        for (String targetName : targetSet) {
            for (String databaseName : databaseSet) {
                for (String tableName : tableSet) {
                    res.add(new BackendTableInfo(targetName, new SchemaInfo(databaseName, tableName)));
                }
            }
        }
        if (res.isEmpty()) {
            return (List) allBackends;
        } else {
            if (allBackends.isEmpty()) {
                return (List) res;
            }
            return res.stream().filter(allBackends::contains).collect(Collectors.toList());
        }
    }

    private List<DataNode> getBackendTableInfosByNatureDatabaseTable(ShardingTableHandler logicTable) {
        List<DataNode> routeIndexSortedSet = Collections.emptyList();
        if (!columnMap.isEmpty()) {
            routeIndexSortedSet = getRouteIndexSortedSet(logicTable.getNatureTableColumnInfo());
        }
        if (routeIndexSortedSet.isEmpty()) {
            return (List) logicTable.getShardingBackends();
        } else {
            return routeIndexSortedSet;
        }
    }

    private List<String> getRouteColumnSortedSet(SimpleColumnInfo.ShardingInfo target) {
        return getRouteIndexSortedSet(target).stream().map(i -> i.getTargetName()).collect(Collectors.toList());
    }

    private List<DataNode> getRouteIndexSortedSet(SimpleColumnInfo.ShardingInfo target) {
        return calculate(target.getFunction(), columnMap.values().stream().flatMap(i -> i.stream()).collect(Collectors.toSet())).stream().sorted().collect(Collectors.toList());
    }

    private List<DataNode> calculate(CustomRuleFunction ruleFunction, Set<RangeVariable> values) {
        Objects.requireNonNull(ruleFunction);
        return ruleFunction.calculate(values);
//        HashSet<DataNode> res = new HashSet<>();
//        for (RangeVariable rangeVariable : value) {
//            String begin = Objects.toString(rangeVariable.getBegin());
//            String end = Objects.toString(rangeVariable.getEnd());
//            switch (rangeVariable.getOperator()) {
//                case EQUAL: {
//                    DataNode calculate = ruleFunction.calculate(begin);
//                    if (calculate == null) {
//                        return Collections.emptySet();
//                    }
//                    res.add(calculate);
//                    break;
//                }
//                case RANGE: {
//                    List<DataNode> calculate = ruleFunction.calculateRange(begin, end);
//                    if (calculate == null || calculate.size() == 0) {
//                        return Collections.emptySet();
//                    }
//                    res.addAll(calculate);
//                    break;
//                }
//            }
//        }
//        return res;
    }
//
//    public String getFilterExpr(List<String> rowOrder) {
//        StringBuilder where = new StringBuilder();
//        for (String columnName : rowOrder) {
//            SortedSet<RangeVariable> value = columnMap.get(columnName);
//            for (RangeVariable rangeVariable : value) {
//                if (where.length() > 0) {
//                    if (rangeVariable.isOr()) {
//                        where.append(" or (");
//                    } else {
//                        where.append(" and (");
//                    }
//                } else {
//                    where.append("  (");
//                }
//                switch (rangeVariable.getOperator()) {
//                    case EQUAL:
//                        where.append(columnName).append(" = ").append(rangeVariable.getBegin());
//                        break;
//                    case RANGE:
//                        where.append(columnName).append(" between ").append(rangeVariable.getBegin()).append(" and ").append(rangeVariable.getEnd());
//                        break;
//                }
//                where.append(" ) ");
//            }
//        }
//        return where.toString();
//    }

    public void merge(DataMappingEvaluator arg) {
        arg.columnMap.forEach((key, value) -> this.getRangeVariables(key).addAll(value));
    }
}
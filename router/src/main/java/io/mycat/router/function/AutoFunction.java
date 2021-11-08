/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.router.function;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import io.mycat.Partition;
import io.mycat.RangeVariable;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;

import java.text.MessageFormat;
import java.util.*;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AutoFunction extends CustomRuleFunction {
    String name;
    int dbNum;
    int tableNum;
    SQLMethodInvokeExpr dbMethod;
    SQLMethodInvokeExpr tableMethod;
    private Set<String> dbKeys;
    private Set<String> tableKeys;
    final ToIntFunction<Object> finalDbFunction;
    final ToIntFunction<Object> finalTableFunction;

    public AutoFunction(int dbNum,
                        int tableNum,
                        SQLMethodInvokeExpr dbMethod,
                        SQLMethodInvokeExpr tableMethod,
                        Set<String> dbKeys,
                        Set<String> tableKeys,
                        final ToIntFunction<Object> finalDbFunction,
                        final ToIntFunction<Object> finalTableFunction,
                        int storeNum) {
        this.dbNum = dbNum;
        this.tableNum = tableNum;
        this.dbMethod = dbMethod;
        this.tableMethod = tableMethod;
        this.dbKeys = dbKeys;
        this.tableKeys = tableKeys;
        this.finalDbFunction = finalDbFunction;
        this.finalTableFunction = finalTableFunction;

        this.name = MessageFormat.format("dbNum:{0} tableNum:{1} dbMethod:{2} tableMethod:{3} storeNum:{4}",
                dbNum, tableNum, extractKey(dbMethod), extractKey(tableMethod), storeNum);
    }

    private static String extractMethodText(SQLMethodInvokeExpr method) {
        return method.toString().replaceAll(extractKey(method), "");
    }

    private static String extractKey(SQLMethodInvokeExpr method) {
        if (method == null) {
            return "null";
        }
        String methodName = method.getMethodName().toUpperCase();
        //DD,MM,MMDD,MOD_HASH,UNI_HASH,WEEK,YYYYDD,YYYYMM,YYYYWEEK
        String key;
        switch (methodName) {
            case "DD":
            case "MM":
            case "MMDD":
            case "MOD_HASH":
            case "UNI_HASH":
            case "WEEK":
            case "YYYYDD":
            case "YYYYMM":
            case "YYYYWEEK":
                key = methodName;
                break;
            case "RANGE_HASH": {
                List<SQLExpr> arguments = method.getArguments();
                SQLExpr sqlExpr = arguments.get(2);
                key = "RANGE_HASH$" + sqlExpr;
                break;
            }
            case "RIGHT_SHIFT": {
                List<SQLExpr> arguments = method.getArguments();
                SQLExpr sqlExpr = arguments.get(1);
                key = "RIGHT_SHIFT" + sqlExpr;
                break;
            }
            default:
                key = method.toString();
        }
        return key;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<Partition> calculate(Map<String, RangeVariable> values) {
        boolean getDbIndex = false;
        int dIndex = 0;

        boolean getTIndex = false;
        int tIndex = 0;

        Optional<Iterable<Integer>> dbRange = Optional.empty();
        Optional<Iterable<Integer>> tableRange = Optional.empty();

        Set<Map.Entry<String, RangeVariable>> entries = values.entrySet();
        for (Map.Entry<String, RangeVariable> e : entries) {
            RangeVariable rangeVariable = e.getValue();
            for (String dbShardingKey : dbKeys) {
                if (SQLUtils.nameEquals(dbShardingKey, e.getKey())) {
                    switch (rangeVariable.getOperator()) {
                        case EQUAL:
                            Object value = rangeVariable.getValue();
                            dIndex = finalDbFunction.applyAsInt(value);
                            getDbIndex = true;
                            if (dIndex < 0) {
                                finalDbFunction.applyAsInt(value);
                                throw new IllegalArgumentException();
                            }
                            break;
                        case RANGE:
                            if (isShardingDbEnum()){
                                dbRange = getRange(rangeVariable, dbNum,dbMethod.getMethodName(), finalDbFunction);

                            }
                            break;
                        default:
                            continue;
                    }
                }
            }
            for (String tableShardingKey : tableKeys) {
                if (SQLUtils.nameEquals(tableShardingKey, e.getKey())) {
                    switch (rangeVariable.getOperator()) {
                        case EQUAL:
                            Object value = rangeVariable.getValue();
                            tIndex = finalTableFunction.applyAsInt(value);
                            getTIndex = true;
                            break;
                        case RANGE:
                            if (isShardingTableEnum()){
                                tableRange = getRange(rangeVariable, tableNum,tableMethod.getMethodName(), finalTableFunction);

                            }
                            break;
                        default:
                            continue;
                    }
                }
            }
        }
        if (getDbIndex && getTIndex) {
            return (List) scanOnlyDbTableIndex(dIndex, tIndex);
        }
        if (getTIndex) {
            return (List) scanOnlyTableIndex(tIndex);
        }
        if (getDbIndex) {
            return (List) scanOnlyDbIndex(dIndex);
        }
        List<Partition> list = scanAll();
        if (dbRange.isPresent() || tableRange.isPresent()) {
            Stream<Partition> stream = list.stream();
            if (dbRange.isPresent()) {
                Iterable<Integer> integers = dbRange.get();
                Set<Integer> set = toSet(integers);
                stream = stream.filter(p -> set.contains(p.getDbIndex()));
            }
            if (tableRange.isPresent()) {
                Iterable<Integer> integers = tableRange.get();
                Set<Integer> set = toSet(integers);
                stream = stream.filter(p -> set.contains(p.getTableIndex()));
            }
            list = stream.collect(Collectors.toList());
        }
        return list;
    }

    public Optional<Iterable<Integer>> getRange(RangeVariable rangeVariable, int size, String name,ToIntFunction<Object> intFunction) {
        Optional<Iterable<Integer>> dbRange = Optional.empty();
        Object begin = rangeVariable.getBegin();
        Object end = rangeVariable.getEnd();
        if (begin != null && end != null) {
            dbRange = Optional.empty();
        }
        return dbRange;
    }

    private Set<Integer> toSet(Iterable<Integer> integers) {
        if (integers instanceof Set) {
            return (Set<Integer>) integers;
        }
        HashSet<Integer> objects = new HashSet<>();
        for (Integer integer : integers) {
            objects.add(integer);
        }

        return objects;
    }

    public abstract List<Partition> scanAll();

    public abstract List<Partition> scanOnlyTableIndex(int index);

    public abstract List<Partition> scanOnlyDbIndex(int index);

    public abstract List<Partition> scanOnlyDbTableIndex(int dbIndex, int tableIndex);

    @Override
    protected void init(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges) {

    }

    @Override
    public boolean isShardingKey(String name) {
        name = SQLUtils.normalize(name);
        return dbKeys.contains(name) || tableKeys.contains(name);
    }

    @Override
    public boolean isShardingDbKey(String name) {
        name = SQLUtils.normalize(name);
        return dbKeys.contains(name);
    }

    @Override
    public boolean isShardingTableKey(String name) {
        name = SQLUtils.normalize(name);
        return tableKeys.contains(name);
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (AutoFunction.class.isAssignableFrom(customRuleFunction.getClass())) {
            AutoFunction ruleFunction = (AutoFunction) customRuleFunction;
            return Objects.equals(this.name, ruleFunction.name);
        }
        return false;
    }

    @Override
    public String getErUniqueID() {
        return name;
    }

    @Override
    public boolean isShardingTargetKey(String name) {
        switch (getShardingTableType()) {
            case SHARDING_INSTANCE_SINGLE_TABLE:
                return isShardingDbKey(name);
            case SINGLE_INSTANCE_SHARDING_TABLE:
                return isShardingTableKey(name);
            case SHARDING_INSTANCE_SHARDING_TABLE:
                return isShardingDbKey(name) || isShardingTableKey(name);
            default:
                throw new IllegalStateException("Unexpected value: " + getShardingTableType());
        }
    }

    @Override
    public boolean isSameTargetFunctionDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction instanceof AutoFunction) {
            AutoFunction left = this;
            AutoFunction right = (AutoFunction) customRuleFunction;
            Set<String> leftTargets = left.scanAll().stream().map(i -> i.getTargetName()).collect(Collectors.toSet());
            Set<String> rightTargets = right.scanAll().stream().map(i -> i.getTargetName()).collect(Collectors.toSet());
            if (leftTargets.equals(rightTargets)) {
                switch (left.getShardingTableType()) {
                    case SHARDING_INSTANCE_SHARDING_TABLE:
                    case SHARDING_INSTANCE_SINGLE_TABLE: {
                        switch (right.getShardingTableType()) {
                            case SHARDING_INSTANCE_SINGLE_TABLE:
                            case SHARDING_INSTANCE_SHARDING_TABLE:
                                return isSameDbFunctionDistribution(right);
                            case SINGLE_INSTANCE_SHARDING_TABLE:
                            default:
                                return false;
                        }
                    }
                    case SINGLE_INSTANCE_SHARDING_TABLE: {
                        switch (right.getShardingTableType()) {
                            case SHARDING_INSTANCE_SINGLE_TABLE:
                            case SHARDING_INSTANCE_SHARDING_TABLE:
                            default:
                                return false;
                            case SINGLE_INSTANCE_SHARDING_TABLE:
                                return true;
                        }
                    }
                    default:
                        return false;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isSameTableFunctionDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction instanceof AutoFunction) {
            AutoFunction left = this;
            AutoFunction right = (AutoFunction) customRuleFunction;
            return left.tableNum == right.tableNum && extractMethodText(left.tableMethod).equalsIgnoreCase(extractMethodText(right.tableMethod));
        }
        return false;
    }

    @Override
    public boolean isSameDbFunctionDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction instanceof AutoFunction) {
            AutoFunction left = this;
            AutoFunction right = (AutoFunction) customRuleFunction;
            return left.dbNum == right.dbNum && extractMethodText(left.dbMethod).equalsIgnoreCase(extractMethodText(right.dbMethod));
        }
        return false;
    }

    @Override
    public boolean isShardingTargetEnum() {
        switch (getShardingTableType()) {
            case SHARDING_INSTANCE_SINGLE_TABLE:
                return isShardingDbEnum();
            case SINGLE_INSTANCE_SHARDING_TABLE:
            case SHARDING_INSTANCE_SHARDING_TABLE:
        }
        return false;
    }
}

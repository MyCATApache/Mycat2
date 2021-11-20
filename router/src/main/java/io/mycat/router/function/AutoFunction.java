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
import org.jetbrains.annotations.NotNull;

import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

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
    final int partitionSize;

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
        this.partitionSize = this.dbNum * this.tableNum;
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

        Optional<Set<Object>> dbRange = Optional.empty();
        Optional<Set<Object>> tableRange = Optional.empty();

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
                            if (isShardingDbEnum()) {
                                dbRange = getRange(rangeVariable, this.partitionSize, dbMethod.getMethodName(), finalDbFunction);

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
                            if (isShardingTableEnum()) {
                                tableRange = getRange(rangeVariable, this.partitionSize, tableMethod.getMethodName(), finalTableFunction);

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

        if (dbRange.isPresent() || tableRange.isPresent()) {
            List<Partition> res = new ArrayList<>();
            if (dbRange.isPresent() && tableRange.isPresent()) {
                Set<Object> dbSet = dbRange.get();
                Set<Object> tableSet = tableRange.get();

                if (dbKeys.equals(tableKeys)) {
                    for (Object localDate : dbSet) {
                        List<Partition> partitions = scanOnlyDbTableIndex(finalDbFunction.applyAsInt(localDate), finalTableFunction.applyAsInt(localDate));
                        res.addAll(partitions);
                    }
                } else {
                    for (Object outer : dbSet) {
                        List<Partition> partitions = scanOnlyDbIndex(finalDbFunction.applyAsInt(outer));
                        for (Object inner : tableSet) {
                            int i = finalTableFunction.applyAsInt(inner);
                            res.addAll(partitions.stream().filter(p->p.getTableIndex() == i).collect(Collectors.toList()));
                        }
                    }
                }
                return res;
            } else if (dbRange.isPresent()) {
                Set<Object> set = dbRange.get();
                for (Object o : set) {
                    res.addAll(scanOnlyDbIndex(finalDbFunction.applyAsInt(o)));
                }
                return res;
            } else if (tableRange.isPresent()) {
                Set<Object> set = tableRange.get();
                for (Object o : set) {
                    res.addAll(scanOnlyTableIndex(finalTableFunction.applyAsInt(o)));
                }
                return res;
            }
        }
        return scanAll();
    }

    public Optional<Set<Object>> getRange(RangeVariable rangeVariable, int limit, String name, ToIntFunction<Object> intFunction) {
        Optional<Set<Object>> dbRange = Optional.empty();
        Object begin = rangeVariable.getBegin();
        Object end = rangeVariable.getEnd();
        if (begin != null && end != null) {
            if ("MM".equalsIgnoreCase(name) || "YYYYMM".equalsIgnoreCase(name)) {
                return enumMonthValue(limit, intFunction, begin, end);
            } else if ("DD".equalsIgnoreCase(name) || "YYYYDD".equalsIgnoreCase(name) || "MMDD".equalsIgnoreCase(name)) {
                return enumDayValue(limit, intFunction, begin, end);
            } else if ("WEEK".equalsIgnoreCase(name) || "YYYYWEEK".equalsIgnoreCase(name)) {
                return enumWeekValue(limit, intFunction, begin, end);
            }
            dbRange = Optional.empty();
        }
        return dbRange;
    }

    @NotNull
    public Optional<Set<Object>> enumWeekValue(int size, ToIntFunction<Object> intFunction, Object begin, Object end) {
        if (begin instanceof LocalDate && end instanceof LocalDate) {
            return enumWeekValue(size, intFunction, (LocalDate) begin, (LocalDate) end);
        } else if (begin instanceof LocalDateTime && end instanceof LocalDateTime) {
            return enumWeekValue(size, intFunction, ((LocalDateTime) begin).toLocalDate(), ((LocalDateTime) end).toLocalDate());
        } else {
            return Optional.empty();
        }
    }

    @NotNull
    public Optional<Set<Object>> enumDayValue(int size, ToIntFunction<Object> intFunction, Object begin, Object end) {
        if (begin instanceof LocalDate && end instanceof LocalDate) {
            return enumDayValue(size, intFunction, (LocalDate) begin, (LocalDate) end);
        } else if (begin instanceof LocalDateTime && end instanceof LocalDateTime) {
            return enumDayValue(size, intFunction, ((LocalDateTime) begin).toLocalDate(), ((LocalDateTime) end).toLocalDate());
        } else {
            return Optional.empty();
        }
    }

    @NotNull
    public Optional<Set<Object>> enumMonthValue(int size, ToIntFunction<Object> intFunction, Object begin, Object end) {
        if (begin instanceof LocalDate && end instanceof LocalDate) {
            return enumMonthValue(size, intFunction, (LocalDate) begin, (LocalDate) end);
        } else if (begin instanceof LocalDateTime && end instanceof LocalDateTime) {
            return enumMonthValue(size, intFunction, ((LocalDateTime) begin).toLocalDate(), ((LocalDateTime) end).toLocalDate());
        } else {
            return Optional.empty();
        }
    }

    @NotNull
    public Optional<Set<Object>> enumMonthValue(int size, ToIntFunction<Object> intFunction, LocalDate begin, LocalDate end) {
        Set<Object> res = new HashSet<>(12);

        LocalDate cur = begin;
        for (int i = 0; i < size && end.isAfter(cur)
                && res.size() < size;//优化
             i++) {
            res.add((cur));
            cur = cur.plusMonths(1);
        }
        if (end.isAfter(cur)) {
            return Optional.empty();
        }
        return Optional.of(res);
    }

    @NotNull
    private Optional<Set<Object>> enumDayValue(int size, ToIntFunction<Object> intFunction, LocalDate begin, LocalDate end) {
        Set<Object> res = new HashSet<>(12);

        LocalDate cur = begin;
        for (int i = 0; i < size && end.isAfter(cur)
                && res.size() < size;//优化
             i++) {
            res.add((cur));
            cur = cur.plusDays(1);
        }
        if (end.isAfter(cur)) {
            return Optional.empty();
        }
        return Optional.of(res);
    }

    @NotNull
    public Optional<Set<Object>> enumWeekValue(int maxRange, ToIntFunction<Object> intFunction, LocalDate begin, LocalDate end) {
        Set<Object> res = new HashSet<>(12);
        LocalDate cur = begin;
        for (int i = 0; i < maxRange && end.isAfter(cur)
                && res.size() < maxRange;//优化
             i++) {
            res.add((cur));
            cur = cur.plusWeeks(1);
        }
        if (end.isAfter(cur)) {
            return Optional.empty();
        }
        return Optional.of(res);
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

    public abstract boolean isFlattenMapping();
}

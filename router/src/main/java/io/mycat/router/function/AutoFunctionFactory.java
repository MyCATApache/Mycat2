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

package io.mycat.router.function;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.parser.SQLExprParser;
import groovy.text.SimpleTemplateEngine;
import groovy.text.Template;
import io.mycat.BackendTableInfo;
import io.mycat.DataNode;
import io.mycat.RangeVariable;
import io.mycat.SimpleColumnInfo;
import io.mycat.config.ShardingFuntion;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.SplitUtil;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.StringWriter;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

public class AutoFunctionFactory {

    @EqualsAndHashCode
    @AllArgsConstructor
    @Getter
    static class Key {
        int dbIndex;
        int tableIndex;
    }

    @SneakyThrows
    public static final CustomRuleFunction
    getTableFunction(ShardingTableHandler tableHandler, ShardingFuntion config) {

        Map<String, Object> properties = config.getProperties();

        int dbNum = Integer.parseInt(properties.getOrDefault("dbNum", 1).toString());
        int tableNum = Integer.parseInt(properties.getOrDefault("tableNum", 1).toString());

        Integer storeNum = Optional.ofNullable(properties.get("storeNum"))
                .map(i -> Integer.parseInt(i.toString()))
                .orElseThrow(() -> new IllegalArgumentException("can not get storeNum"));

        Integer storeDbNum = Optional.ofNullable(properties.get("storeDbNum"))
                .map(i -> Integer.parseInt(i.toString())).orElse(dbNum * tableNum / storeNum);
        SQLMethodInvokeExpr tableMethod = converyToMethodExpr((String) properties.get("tableMethod"));
        SQLMethodInvokeExpr dbMethod = converyToMethodExpr((String) properties.get("dbMethod"));
        String sep = "/";
        String erUniqueName = Optional.ofNullable(dbMethod).map(i->i.getMethodName()).orElse("")
                +Optional.ofNullable(tableMethod).map(i->i.getMethodName()).orElse("")
                +" storeNum:"+storeNum+" storeDbNum:"+storeDbNum+" dbNum:"+dbNum+" tableNum:"+tableNum;
        String mappingFormat = (String) properties.getOrDefault("mappingFormat",
                String.join(sep, "c${targetIndex}",
                        tableHandler.getSchemaName() + "_${dbIndex}",
                        tableHandler.getTableName() + "_${tableIndex}"));
        Map<Integer, List<IndexDataNode>> datanodes = new HashMap<>();
        List<int[]> seq = new ArrayList<>();
        int tableCount = 0;
        for (int dbIndex = 0; dbIndex < dbNum; dbIndex++) {
            for ( int tableIndex = 0; tableIndex < tableNum; tableCount++,tableIndex++) {
                seq.add(new int[]{dbIndex, tableCount,tableIndex});
            }
        }

        SimpleTemplateEngine templateEngine = new SimpleTemplateEngine();
        Template template = templateEngine.createTemplate(mappingFormat);
        HashMap<String, Object> context = new HashMap<>(properties);


        Map<Key, DataNode> cache = new ConcurrentHashMap<>();
        for (int i = 0; i < seq.size(); i++) {
            int seqIndex = i / storeDbNum;
            int[] ints = seq.get(i);
            int currentDbIndex = ints[0];
            int currentTableCount= ints[1];
            int currentTableIndex = ints[2];
            context.put("targetIndex", String.valueOf(seqIndex));
            context.put("dbIndex", String.valueOf(currentDbIndex));
            context.put("tableIndex", String.valueOf(currentTableIndex));
            StringWriter stringWriter = new StringWriter();
            template.make(context).writeTo(stringWriter);
            String[] strings = SplitUtil.split(stringWriter.getBuffer().toString(), sep);

            IndexDataNode backendTableInfo = new IndexDataNode(strings[0], strings[1], strings[2], currentDbIndex, currentTableCount);
            cache.put(new Key(ints[0], ints[1]), backendTableInfo);
            List<IndexDataNode> nodeList = datanodes.computeIfAbsent(currentDbIndex, (k) -> new ArrayList<>());
            nodeList.add(backendTableInfo);
        }


        ToIntFunction<Object> tableFunction = (o) -> 0;
        Set<String> dbShardingKeys = new HashSet<>();

        ToIntFunction<Object> dbFunction = (o) -> 0;
        Set<String> tableShardingKeys = new HashSet<>();

        if (dbMethod != null) {
            int num = dbNum;
            SQLMethodInvokeExpr methodInvokeExpr = dbMethod;

            if (SQLUtils.nameEquals("HASH", methodInvokeExpr.getMethodName())) {
                methodInvokeExpr.setMethodName("MOD_HASH");
            }
            if (SQLUtils.nameEquals("MOD_HASH", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                dbShardingKeys.add(shardingKey);
                SimpleColumnInfo columnInfo = tableHandler.getColumnByName(shardingKey);
                dbFunction = specilizeSingleModHash(num, columnInfo);
            }
            if (SQLUtils.nameEquals("UNI_HASH", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                dbShardingKeys.add(shardingKey);
                SimpleColumnInfo columnInfo = tableHandler.getColumnByName(shardingKey);
                dbFunction = specilizeSingleModHash(num, columnInfo);
            }
            if (SQLUtils.nameEquals("RIGHT_SHIFT", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                dbShardingKeys.add(shardingKey);

                int shift = Integer.parseInt(getShardingKey(methodInvokeExpr, 1));
                SimpleColumnInfo columnInfo = tableHandler.getColumnByName(shardingKey);
                dbFunction = specilizeSingleRightShift(num, shift, columnInfo);
            }

            if (SQLUtils.nameEquals("RANGE_HASH", methodInvokeExpr.getMethodName())) {
                String shardingKey1 = getShardingKey(methodInvokeExpr);
                dbShardingKeys.add(shardingKey1);

                String shardingKey2 = getShardingKey(methodInvokeExpr, 1);
                dbShardingKeys.add(shardingKey2);

                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey1)
                );
                SimpleColumnInfo column2 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey2)
                );

                if (column1.getType() == column2.getType()) {
                    throw new IllegalArgumentException();
                }
                int n;
                if (methodInvokeExpr.getArguments().size() > 2) {
                    n = Integer.parseInt(getShardingKey(methodInvokeExpr, 2));
                } else {
                    n = 0;
                }
                dbFunction = specilizeSingleRangeHash(num, n, column1);
            }
            if (SQLUtils.nameEquals("YYYYDD", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                dbShardingKeys.add(shardingKey);

                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );

                dbFunction = specilizeyyyydd(num, column1);
            }
            if (SQLUtils.nameEquals("YYYYWEEK", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                dbShardingKeys.add(shardingKey);
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                dbFunction = specilizeyyyyWeek(num, column1);
            }
            if ("STR_HASH".equalsIgnoreCase(methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                dbShardingKeys.add(shardingKey);

                List<SQLExpr> arguments = methodInvokeExpr.getArguments();
                int startIndex;
                int endIndex;
                int valType;
                int randSeed;
                if (arguments.size() >= 3) {
                    startIndex = Integer.parseInt(Objects.toString(arguments.get(1)));
                    endIndex = Integer.parseInt(Objects.toString(arguments.get(2)));
                } else {
                    startIndex = -1;
                    endIndex = -1;
                }
                if (arguments.size() >= 4) {
                    valType = Integer.parseInt(Objects.toString(arguments.get(3)));
                } else {
                    valType = 0;
                }
                if (arguments.size() >= 5) {
                    randSeed = Integer.parseInt(Objects.toString(arguments.get(4)));
                } else {
                    randSeed = 31;
                }
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                dbFunction = specilizeStrHash(num, startIndex, endIndex, valType, randSeed, column1);
            }

        }
        if (tableMethod != null) {
            int num = tableNum;
            SQLMethodInvokeExpr methodInvokeExpr = tableMethod;
            if (SQLUtils.nameEquals("HASH", methodInvokeExpr.getMethodName())) {
                methodInvokeExpr.setMethodName("MOD_HASH");
            }
            if (SQLUtils.nameEquals("MOD_HASH", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                tableShardingKeys.add(shardingKey);
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableFunction = specilizeSingleModHash(num, column1);
            }
            if (SQLUtils.nameEquals("UNI_HASH", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableFunction = specilizeSingleModHash(num, column1);
            }
            if (SQLUtils.nameEquals("RIGHT_SHIFT", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                tableShardingKeys.add(shardingKey);
                int shift = Integer.parseInt(getShardingKey(methodInvokeExpr, 1));
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableFunction = specilizeSingleRightShift(num, shift, column1);
            }
            if (SQLUtils.nameEquals("RANGE_HASH", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                tableShardingKeys.add(shardingKey);
                tableShardingKeys.add(getShardingKey(methodInvokeExpr, 1));
                int n;
                if (methodInvokeExpr.getArguments().size() > 2) {
                    n = Integer.parseInt(getShardingKey(methodInvokeExpr, 2));
                } else {
                    n = 0;
                }
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableFunction = specilizeSingleRangeHash(num, n, column1);
            }
            if (SQLUtils.nameEquals("YYYYMM", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                tableShardingKeys.add(shardingKey);
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableFunction = specilizeyyyymm(num, column1);
            }
            if (SQLUtils.nameEquals("YYYYDD", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                tableShardingKeys.add(shardingKey);
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableFunction = specilizeyyyydd(num, column1);
            }
            if (SQLUtils.nameEquals("YYYYWEEK", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                tableShardingKeys.add(shardingKey);
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableFunction = specilizeyyyyWeek(num, column1);
            }
            if (SQLUtils.nameEquals("WEEK", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableShardingKeys.add(shardingKey);
                tableFunction = specilizeWeek(num, column1);
            }
            if (SQLUtils.nameEquals("MMDD", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableShardingKeys.add(shardingKey);
                tableFunction = specilizemmdd(num, column1);
            }
            if (SQLUtils.nameEquals("DD", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableShardingKeys.add(shardingKey);
                tableFunction = specilizedd(num, column1);
            }
            if (SQLUtils.nameEquals("MM", methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableShardingKeys.add(shardingKey);
                tableFunction = specilizemm(num, column1);
            }
            if ("STR_HASH".equalsIgnoreCase(methodInvokeExpr.getMethodName())) {
                String shardingKey = getShardingKey(methodInvokeExpr);
                SimpleColumnInfo column1 = Objects.requireNonNull(
                        tableHandler.getColumnByName(shardingKey)
                );
                tableShardingKeys.add(shardingKey);
                List<SQLExpr> arguments = methodInvokeExpr.getArguments();
                int startIndex;
                int endIndex;
                int valType;
                int randSeed;
                if (arguments.size() >= 3) {
                    startIndex = Integer.parseInt(Objects.toString(arguments.get(1)));
                    endIndex = Integer.parseInt(Objects.toString(arguments.get(2)));
                } else {
                    startIndex = -1;
                    endIndex = -1;
                }
                if (arguments.size() >= 4) {
                    valType = Integer.parseInt(Objects.toString(arguments.get(3)));
                } else {
                    valType = 0;
                }
                if (arguments.size() >= 5) {
                    randSeed = Integer.parseInt(Objects.toString(arguments.get(4)));
                } else {
                    randSeed = 31;
                }
                tableFunction = specilizeStrHash(num, startIndex, endIndex, valType, randSeed, column1);
            }
        }
        if (dbMethod != null && tableMethod != null) {
            if (SQLUtils.nameEquals("MOD_HASH", dbMethod.getMethodName())) {
                if (SQLUtils.nameEquals("MOD_HASH", tableMethod.getMethodName())) {
                    String tableShardingKey = Objects.requireNonNull(getShardingKey(tableMethod));
                    String dbShardingKey = getShardingKey(dbMethod);

                    SimpleColumnInfo tableColumn = tableHandler.getColumnByName(tableShardingKey);
                    SimpleColumnInfo dbColumn = tableHandler.getColumnByName(dbShardingKey);

                    tableShardingKeys.add(tableShardingKey);
                    dbShardingKeys.add(dbShardingKey);



                    if (tableShardingKey.equalsIgnoreCase(dbShardingKey)) {
                        int total = dbNum * tableNum;
                        tableFunction = (o) -> {
                            o = tableColumn.normalizeValue(o);
                            if (o == null) o = 0;
                            if (o instanceof Number) {
                                long l = ((Number) o).longValue();
                                long i = l % total;
                                if (i < 0) {
                                    throw new IllegalArgumentException();
                                }
                                return (int) i;
                            }
                            if (o instanceof String) {
                                return hashCode((String) o) % total;
                            }
                            throw new UnsupportedOperationException();
                        };
                        ToIntFunction<Object> function = tableFunction;
                        dbFunction = (o) -> {
                          return   function.applyAsInt(o)/tableNum;
                        };
                    } else {
                        tableFunction = specilizeSingleModHash(tableNum, tableColumn);
                    }

                }
            }
            if (SQLUtils.nameEquals("UNI_HASH", dbMethod.getMethodName())) {
                if (SQLUtils.nameEquals("UNI_HASH", tableMethod.getMethodName())) {
                    String tableShardingKey = Objects.requireNonNull(getShardingKey(tableMethod));
                    String dbShardingKey = getShardingKey(dbMethod);
                    SimpleColumnInfo tableColumn = tableHandler.getColumnByName(tableShardingKey);
                    SimpleColumnInfo dbColumn = tableHandler.getColumnByName(dbShardingKey);
                    dbShardingKeys.add(dbShardingKey);
                    tableShardingKeys.add(tableShardingKey);

                    dbFunction = specilizeSingleModHash(dbNum, dbColumn);

                    if (tableShardingKey.equalsIgnoreCase(dbShardingKey)) {
                        tableFunction = (o) -> {
                            o = tableColumn.normalizeValue(o);
                            int total = dbNum * tableNum;
                            if (o instanceof Number) {
                                long intValue = ((Number) o).longValue();

                                long l = (intValue) % dbNum * tableNum
                                        +
                                        (intValue / dbNum) % tableNum;
                                return (int) l;
                            }
                            if (o instanceof String) {
                                return hashCode((String) o) % total / tableNum;
                            }
                            throw new UnsupportedOperationException();
                        };
                    } else {
                        tableFunction = specilizeSingleModHash(tableNum, tableColumn);
                    }
                }

            }
        }
        final ToIntFunction<Object> finalDbFunction = dbFunction;
        final ToIntFunction<Object> finalTableFunction = tableFunction;


        Function<Map<String, Collection<RangeVariable>>, List<DataNode>> function = new Function<Map<String, Collection<RangeVariable>>, List<DataNode>>() {
            @Override
            public List<DataNode> apply(Map<String, Collection<RangeVariable>> stringCollectionMap) {
                boolean getDbIndex = false;
                int dIndex = 0;

                boolean getTIndex = false;
                int tIndex = 0;

                Set<Map.Entry<String, Collection<RangeVariable>>> entries = stringCollectionMap.entrySet();
                for (Map.Entry<String, Collection<RangeVariable>> e : entries) {
                    for (String dbShardingKey : dbShardingKeys) {
                        if (SQLUtils.nameEquals(dbShardingKey, e.getKey())) {
                            Collection<RangeVariable> rangeVariables = e.getValue();
                            if (rangeVariables.size() != 1) {
                                break;
                            }
                            if (rangeVariables != null && !rangeVariables.isEmpty()) {
                                for (RangeVariable rangeVariable : rangeVariables) {
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
                                        default:
                                            continue;
                                    }
                                }
                            }
                        }
                    }
                    for (String tableShardingKey : tableShardingKeys) {
                        if (SQLUtils.nameEquals(tableShardingKey, e.getKey())) {
                            Collection<RangeVariable> rangeVariables = e.getValue();
                            if (rangeVariables.size() != 1) {
                                break;
                            }
                            if (rangeVariables != null && !rangeVariables.isEmpty()) {
                                for (RangeVariable rangeVariable : rangeVariables) {
                                    switch (rangeVariable.getOperator()) {
                                        case EQUAL:
                                            Object value = rangeVariable.getValue();
                                            tIndex = finalTableFunction.applyAsInt(value);
                                            getTIndex = true;
                                            break;
                                        case RANGE:
                                        default:
                                            continue;
                                    }
                                }
                            }
                        }
                    }
                }
                if (getDbIndex && getTIndex) {
                    List<IndexDataNode> indexDataNodes = Objects.requireNonNull((List) datanodes.get(dIndex));
                    for (IndexDataNode indexDataNode : indexDataNodes) {
                        if(indexDataNode.getTableIndex() ==  tIndex){
                            return Collections.singletonList(indexDataNode);
                        }
                    }
                }
                if (getDbIndex) {
                    return new ArrayList<>((List) datanodes.get(dIndex));
                }
                if (getTIndex) {
                    for (List<IndexDataNode> value : datanodes.values()) {
                        for (IndexDataNode indexDataNode : value) {
                            if(indexDataNode.getTableIndex() ==  tIndex){
                                return Collections.singletonList(indexDataNode);
                            }
                        }
                    }
                }
                return datanodes.values().stream().flatMap(i -> i.stream()).collect(Collectors.toList());
            }
        };


        return new AutoFunction(dbNum, tableNum, dbMethod, tableMethod,dbShardingKeys,tableShardingKeys,function,erUniqueName);
    }

    @NotNull
    public static ToIntFunction<Object> specilizemm(int num, SimpleColumnInfo column1) {
        ToIntFunction<Object> tableFunction;
        switch (column1.getType()) {
            case NUMBER:
                tableFunction = o -> mm(num, (Number) column1.normalizeValue(o));
                break;
            case STRING:
                tableFunction = o -> mm(num, (String) column1.normalizeValue(o));
                break;
            case BLOB:
                tableFunction = o -> mm(num, (byte[]) column1.normalizeValue(o));
                break;
            case TIME:
                tableFunction = o -> mm(num, (Duration) column1.normalizeValue(o));
                break;
            case DATE:
                tableFunction = o -> mm(num, (LocalDate) column1.normalizeValue(o));
                break;
            case TIMESTAMP:
                tableFunction = o -> mm(num, (LocalDateTime) column1.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + column1.getType());
        }
        return tableFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizedd(int num, SimpleColumnInfo column1) {
        ToIntFunction<Object> tableFunction;
        switch (column1.getType()) {
            case NUMBER:
                tableFunction = o -> dd(num, (Number) column1.normalizeValue(o));
                break;
            case STRING:
                tableFunction = o -> dd(num, (String) column1.normalizeValue(o));
                break;
            case BLOB:
                tableFunction = o -> dd(num, (byte[]) column1.normalizeValue(o));
                break;
            case TIME:
                tableFunction = o -> dd(num, (Duration) column1.normalizeValue(o));
                break;
            case DATE:
                tableFunction = o -> dd(num, (LocalDate) column1.normalizeValue(o));
                break;
            case TIMESTAMP:
                tableFunction = o -> dd(num, (LocalDateTime) column1.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + column1.getType());
        }
        return tableFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizemmdd(int num, SimpleColumnInfo column1) {
        ToIntFunction<Object> tableFunction;
        switch (column1.getType()) {
            case NUMBER:
                tableFunction = o -> mmdd(num, (Number) column1.normalizeValue(o));
                break;
            case STRING:
                tableFunction = o -> mmdd(num, (String) column1.normalizeValue(o));
                break;
            case BLOB:
                tableFunction = o -> mmdd(num, (byte[]) column1.normalizeValue(o));
                break;
            case TIME:
                tableFunction = o -> mmdd(num, (Duration) column1.normalizeValue(o));
                break;
            case DATE:
                tableFunction = o -> mmdd(num, (LocalDate) column1.normalizeValue(o));
                break;
            case TIMESTAMP:
                tableFunction = o -> mmdd(num, (LocalDateTime) column1.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + column1.getType());
        }
        return tableFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizeWeek(int num, SimpleColumnInfo column1) {
        ToIntFunction<Object> tableFunction;
        switch (column1.getType()) {
            case NUMBER:
                tableFunction = o -> week(num, (Number) column1.normalizeValue(o));
                break;
            case STRING:
                tableFunction = o -> week(num, (String) column1.normalizeValue(o));
                break;
            case BLOB:
                tableFunction = o -> week(num, (byte[]) column1.normalizeValue(o));
                break;
            case TIME:
                tableFunction = o -> week(num, (Duration) column1.normalizeValue(o));
                break;
            case DATE:
                tableFunction = o -> week(num, (LocalDate) column1.normalizeValue(o));
                break;
            case TIMESTAMP:
                tableFunction = o -> week(num, (LocalDateTime) column1.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + column1.getType());
        }
        return tableFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizeyyyymm(int num, SimpleColumnInfo column1) {
        ToIntFunction<Object> tableFunction;
        switch (column1.getType()) {
            case NUMBER:
                tableFunction = o -> yyyymm(num, (Number) column1.normalizeValue(o));
                break;
            case STRING:
                tableFunction = o -> yyyymm(num, (String) column1.normalizeValue(o));
                break;
            case BLOB:
                tableFunction = o -> yyyymm(num, (byte[]) column1.normalizeValue(o));
                break;
            case TIME:
                tableFunction = o -> yyyymm(num, (Duration) column1.normalizeValue(o));
                break;
            case DATE:
                tableFunction = o -> yyyymm(num, (LocalDate) column1.normalizeValue(o));
                break;
            case TIMESTAMP:
                tableFunction = o -> yyyymm(num, (LocalDateTime) column1.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + column1.getType());
        }
        return tableFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizeSingleDivHash(int num, SimpleColumnInfo columnInfo) {
        ToIntFunction<Object> dbFunction;
        switch (columnInfo.getType()) {
            case NUMBER:
                dbFunction = o -> singleDivHash(num, (Number) columnInfo.normalizeValue(o));
                break;
            case STRING:
                dbFunction = o -> singleDivHash(num, (String) columnInfo.normalizeValue(o));
                break;
            case BLOB:
                dbFunction = o -> singleDivHash(num, (byte[]) columnInfo.normalizeValue(o));
                break;
            case TIME:
                dbFunction = o -> singleDivHash(num, (Duration) columnInfo.normalizeValue(o));
                break;
            case DATE:
                dbFunction = o -> singleDivHash(num, (LocalDate) columnInfo.normalizeValue(o));
                break;
            case TIMESTAMP:
                dbFunction = o -> singleDivHash(num, (LocalDateTime) columnInfo.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + columnInfo.getType());
        }
        return dbFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizeSingleModHash(int num, SimpleColumnInfo columnInfo) {
        ToIntFunction<Object> dbFunction;
        switch (columnInfo.getType()) {
            case NUMBER:
                dbFunction = o -> singleModHash(num, (Number) columnInfo.normalizeValue(o));
                break;
            case STRING:
                dbFunction = o -> singleModHash(num, (String) columnInfo.normalizeValue(o));
                break;
            case BLOB:
                dbFunction = o -> singleModHash(num, (byte[]) columnInfo.normalizeValue(o));
                break;
            case TIME:
                dbFunction = o -> singleModHash(num, (Duration) columnInfo.normalizeValue(o));
                break;
            case DATE:
                dbFunction = o -> singleModHash(num, (LocalDate) columnInfo.normalizeValue(o));
                break;
            case TIMESTAMP:
                dbFunction = o -> singleModHash(num, (LocalDateTime) columnInfo.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + columnInfo.getType());
        }
        return dbFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizeSingleRightShift(int num, int shift, SimpleColumnInfo columnInfo) {
        ToIntFunction<Object> dbFunction;
        switch (columnInfo.getType()) {
            case NUMBER:
                dbFunction = o -> singleRightShift(num, shift, (Number) columnInfo.normalizeValue(o));
                break;
            case STRING:
                dbFunction = o -> singleRightShift(num, shift, (String) columnInfo.normalizeValue(o));
                break;
            case BLOB:
                dbFunction = o -> singleRightShift(num, shift, (byte[]) columnInfo.normalizeValue(o));
                break;
            case TIME:
                dbFunction = o -> singleRightShift(num, shift, (Duration) columnInfo.normalizeValue(o));
                break;
            case DATE:
                dbFunction = o -> singleRightShift(num, shift, (LocalDate) columnInfo.normalizeValue(o));
                break;
            case TIMESTAMP:
                dbFunction = o -> singleRightShift(num, shift, (LocalDateTime) columnInfo.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + columnInfo.getType());
        }
        return dbFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizeSingleRangeHash(int num, int n, SimpleColumnInfo column1) {
        ToIntFunction<Object> dbFunction;
        switch (column1.getType()) {
            case NUMBER:
                dbFunction = o -> singleRangeHash(num, (Number) column1.normalizeValue(o));
                break;
            case STRING:
                dbFunction = o -> singleRangeHash(num, n, (String) column1.normalizeValue(o));
                break;
            case BLOB:
                dbFunction = o -> singleRangeHash(num, n, (byte[]) column1.normalizeValue(o));
                break;
            case TIME:
                dbFunction = o -> singleRangeHash(num, n, (Duration) column1.normalizeValue(o));
                break;
            case DATE:
                dbFunction = o -> singleRangeHash(num, n, (LocalDate) column1.normalizeValue(o));
                break;
            case TIMESTAMP:
                dbFunction = o -> singleRangeHash(num, n, (LocalDateTime) column1.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + column1.getType());
        }
        return dbFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizeyyyydd(int num, SimpleColumnInfo column1) {
        ToIntFunction<Object> dbFunction;
        switch (column1.getType()) {
            case NUMBER:
                dbFunction = o -> yyyydd(num, (Number) column1.normalizeValue(o));
                break;
            case STRING:
                dbFunction = o -> yyyydd(num, (String) column1.normalizeValue(o));
                break;
            case BLOB:
                dbFunction = o -> yyyydd(num, (byte[]) column1.normalizeValue(o));
                break;
            case TIME:
                dbFunction = o -> yyyydd(num, (Duration) column1.normalizeValue(o));
                break;
            case DATE:
                dbFunction = o -> yyyydd(num, (LocalDate) column1.normalizeValue(o));
                break;
            case TIMESTAMP:
                dbFunction = o -> yyyydd(num, (LocalDateTime) column1.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + column1.getType());
        }
        return dbFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizeyyyyWeek(int num, SimpleColumnInfo column1) {
        ToIntFunction<Object> dbFunction;
        switch (column1.getType()) {
            case NUMBER:
                dbFunction = o -> yyyyWeek(num, (Number) column1.normalizeValue(o));
                break;
            case STRING:
                dbFunction = o -> yyyyWeek(num, (String) column1.normalizeValue(o));
                break;
            case BLOB:
                dbFunction = o -> yyyyWeek(num, (byte[]) column1.normalizeValue(o));
                break;
            case TIME:
                dbFunction = o -> yyyyWeek(num, (Duration) column1.normalizeValue(o));
                break;
            case DATE:
                dbFunction = o -> yyyyWeek(num, (LocalDate) column1.normalizeValue(o));
                break;
            case TIMESTAMP:
                dbFunction = o -> yyyyWeek(num, (LocalDateTime) column1.normalizeValue(o));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + column1.getType());
        }
        return dbFunction;
    }

    @NotNull
    public static ToIntFunction<Object> specilizeStrHash(int num, int startIndex, int endIndex, int valType, int randSeed, SimpleColumnInfo column1) {
        ToIntFunction<Object> dbFunction;
        switch (column1.getType()) {
            case NUMBER:
                dbFunction = value -> strHash(num, startIndex, endIndex, valType, randSeed,
                        (Number) column1.normalizeValue(value));
                break;
            case STRING:
                dbFunction = value -> strHash(num, startIndex, endIndex, valType, randSeed,
                        (String) column1.normalizeValue(value));
                break;
            case BLOB:
                dbFunction = value -> strHash(num, startIndex, endIndex, valType, randSeed,
                        (byte[]) column1.normalizeValue(value));
                break;
            case TIME:
                dbFunction = value -> strHash(num, startIndex, endIndex, valType, randSeed,
                        (Duration) column1.normalizeValue(value));
                break;
            case DATE:
                dbFunction = value -> strHash(num, startIndex, endIndex, valType, randSeed,
                        (LocalDate) column1.normalizeValue(value));
                break;
            case TIMESTAMP:
                dbFunction = value -> strHash(num, startIndex, endIndex, valType, randSeed,
                        (LocalDateTime) column1.normalizeValue(value));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + column1.getType());
        }
        return dbFunction;
    }

//    @NotNull
//    public static ToIntFunction<Object> specilizeSingleRemainderHash(int num, SimpleColumnInfo column1) {
//        ToIntFunction<Object> tableFunction;
//        switch (column1.getType()) {
//            case NUMBER:
//                tableFunction = o -> singleRemainderHash(num, (Number) column1.normalizeValue(o));
//                break;
//            case STRING:
//                tableFunction = o -> singleRemainderHash(num, (String) column1.normalizeValue(o));
//                break;
//            case BLOB:
//                tableFunction = o -> singleRemainderHash(num, (byte[]) column1.normalizeValue(o));
//                break;
//            case TIME:
//                tableFunction = o -> singleRemainderHash(num, (Duration) column1.normalizeValue(o));
//                break;
//            case DATE:
//                tableFunction = o -> singleRemainderHash(num, (LocalDate) column1.normalizeValue(o));
//                break;
//            case TIMESTAMP:
//                tableFunction = o -> singleRemainderHash(num, (LocalDateTime) column1.normalizeValue(o));
//                break;
//            default:
//                throw new IllegalStateException("Unexpected value: " + column1.getType());
//        }
//        return tableFunction;
//    }

    public static int mm(int num, Object o) {
        if (o == null) return 0;
        long mm;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            mm = localDate.getMonthValue();
        } else if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            mm = localDateTime.getMonthValue();
        } else {
            throw new UnsupportedOperationException();
        }
        return (int) (mm % num);
    }

    public static int dd(int num, Object o) {
        if (o == null) return 0;
        long day;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            day = localDate.getDayOfMonth();
        } else if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            day = localDateTime.getDayOfMonth();
        } else {
            throw new UnsupportedOperationException();
        }
        return (int) (day % num);
    }

    public static int mmdd(int num, Object o) {
        if (o == null) return 0;
        long day;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            day = localDate.getDayOfYear();
        } else if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            day = localDateTime.getDayOfYear();
        } else {
            throw new UnsupportedOperationException();
        }
        return (int) ((day) % num);
    }

    public static int strHash(int num, int startIndex, int endIndex, int valType, int randSeed, Object value) {
        if (value == null) value = "null";
        String s = mySubstring(startIndex, endIndex, value.toString());
        if (valType == 0) {
            return hashCode(s, randSeed) % num;
        }
        if (valType == 1) {
            return (int) (Long.parseLong(s) % num);
        }
        throw new UnsupportedOperationException();
    }

    public static int yyyyWeek(int num, Object o) {
        if (o == null) return 0;
        long YYYY;
        long WEEK;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            YYYY = localDate.getYear();
            WEEK = localDate.get(WeekFields.ISO.weekOfWeekBasedYear());
        } else if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            YYYY = localDateTime.getYear();
            WEEK = localDateTime.get(WeekFields.ISO.weekOfWeekBasedYear());
        } else {
            throw new UnsupportedOperationException();
        }
        return (int) ((YYYY * 54 + WEEK) % num);
    }

    public static int week(int num, Object o) {
        if (o == null) return 0;
        long day;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            day = localDate.getDayOfWeek().getValue();
        } else if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            day = localDateTime.getDayOfWeek().getValue();
        } else {
            throw new UnsupportedOperationException();
        }
        return (int) ((day) % num);
    }

    public static int yyyydd(int num, Object o) {
        if (o == null) return 0;
        long YYYY;
        long DD;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            YYYY = localDate.getYear();
            DD = localDate.getDayOfYear();
        } else if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            YYYY = localDateTime.getYear();
            DD = localDateTime.getDayOfYear();
        } else {
            throw new UnsupportedOperationException();
        }
        return (int) ((YYYY * 366 + DD) % num);
    }

    public static int yyyymm(int num, Object o) {
        if (o == null) return 0;
        Integer YYYY = null;
        Integer MM = null;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            YYYY = localDate.getYear();
            MM = localDate.getMonthValue();
        }
        if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            YYYY = localDateTime.getYear();
            MM = localDateTime.getMonthValue();
        }
        if (YYYY == null && MM == null) {
            throw new UnsupportedOperationException();
        }
        return (YYYY * 12 + MM) % num;
    }

    public static int singleRangeHash(int num, int n, Object o) {

        if (o instanceof Number) {
            return singleRangeHash(num, (Number) o);
        }
        if (o instanceof String) {
            return singleRangeHash(num, n, (String) o);
        }
        throw new UnsupportedOperationException();
    }

    public static int singleRangeHash(int num, int n, String o) {
        if (o == null) o = "null";
        return hashCode(o.substring(n)) % num;
    }

    public static int singleRangeHash(int num, Number o) {
        if (o == null) o = 0;
        return (int) (o.longValue() % num);
    }

    public static int singleRightShift(int num, int shift, Object o) {
        if (o instanceof Number) {
            return singleRightShift(num, shift, (Number) o);
        }
        if (o instanceof String) {
            return singleRightShift(num, shift, (String) o);
        }
        throw new UnsupportedOperationException();
    }

    public static int singleRightShift(int num, int shift, String o) {
        if (o == null) o = "null";
        return hashCode(o) >> shift % num;
    }

    public static int singleRightShift(int num, int shift, Number o) {
        return (int) (o.longValue() >> shift % num);
    }

    public static int singleModHash(int num, Object o) {
        if (o instanceof Number) {
            return singleModHash(num, (Number) o);
        }
        if (o instanceof String) {
            return singleModHash(num, (String) o);
        }
        throw new UnsupportedOperationException();
    }

    public static int singleModHash(int num, String o) {
        if (o == null) o = "null";
        return singleModHash(hashCode(o), num);
    }

    public static int singleModHash(int num, Number o) {
        if (o == null) {
            o = 0;
        }
        return (int) (o.longValue() % num);
    }

    public static int singleDivHash(int num, Number o) {
        if (o == null) {
            o = 0;
        }
        return (int) (o.longValue() / num);
    }
    public static int singleDivHash(int num, String o) {
        if (o == null) {
            o = "null";
        }
        return (int) (hashCode(o) / num);
    }
    public static int singleDivHash(int num, Object o) {
        if (o instanceof Number) {
            return singleDivHash(num, (Number) o);
        }
        if (o instanceof String) {
            return singleDivHash(num, (String) o);
        }
        throw new UnsupportedOperationException();
    }
//    public static int singleRemainderHash(int num, Object o) {
//        if (o instanceof Number) {
//            return singleRemainderHash(num, (Number) o);
//        }
//
//        if (o instanceof String) {
//            return singleRemainderHash(num, (String) o);
//        }
//        return singleRemainderHash(num,Objects.toString(o));
//    }

    public static int singleRemainderHash(int num, String o) {
        if (o == null) o = "null";
        return hashCode(o) % num;
    }

    public static int singleRemainderHash(int num, Number o) {
        if (o == null) {
            o = 1;
        }
        long l = o.longValue();
        long l1 = l % num;
        return (int) l1;
    }

    @Nullable
    public static String getShardingKey(SQLMethodInvokeExpr methodInvokeExpr) {
        return getShardingKey(methodInvokeExpr, 0);
    }

    public static String getShardingKey(SQLMethodInvokeExpr methodInvokeExpr, int index) {
        return SQLUtils
                .normalize(methodInvokeExpr.getArguments().get(index).toString());
    }


    public static SQLMethodInvokeExpr converyToMethodExpr(String methodExpr) {
        if (methodExpr == null) return null;
        SQLExprParser sqlExprParser = new SQLExprParser(methodExpr);
        return (SQLMethodInvokeExpr) sqlExprParser.expr();
    }

    public static int hashCode(String value) {
        if (value == null) value = "null";
        return hashCode(value, 31);
    }

    public static int hashCode(String value, int randSeed) {
        int h = 0;
        for (int i = 0; i < value.length(); i++) {
            h = randSeed * h + value.charAt(i);
        }
        return h;
    }

    public static String mySubstring(int startIndex,
                                     int endIndex, String value) {
        if (value == null) value = "null";
        if (startIndex >= 0 && endIndex >= 0 && endIndex > startIndex) {
            return value.substring(Math.min(value.length(), startIndex), Math.min(value.length(), endIndex));
        }
        if (startIndex == -1 && endIndex >= 0) {
            if (value.length() < endIndex) {
                return value;
            }
            return value.substring(endIndex, value.length());
        }
        if (startIndex >= 0 && endIndex == -1) {
            if (value.length() < startIndex) {
                return value;
            }
            return value.substring(0, startIndex);
        }
        return value;

    }

    @Getter
    static class IndexDataNode extends BackendTableInfo {

        private final int dbIndex;
        private final int tableIndex;

        public IndexDataNode(String targetName, String targetSchema, String targetTable,
                             int dbIndex, int tableIndex) {
            super(targetName, targetSchema, targetTable);
            this.dbIndex = dbIndex;
            this.tableIndex = tableIndex;
        }
    }
}

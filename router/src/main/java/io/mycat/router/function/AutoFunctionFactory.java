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

import com.alibaba.fastsql.sql.parser.SQLExprParser;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
import groovy.text.SimpleTemplateEngine;
import groovy.text.Template;
import io.mycat.*;
import io.mycat.config.ShardingFuntion;
import io.mycat.replica.ReplicaSwitchType;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.SplitUtil;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.Nullable;

import java.io.StringWriter;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.ToIntFunction;

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
        int dbNum = Integer.parseInt(properties.getOrDefault("dbNum", 8).toString());
        int tableNum = Integer.parseInt(properties.getOrDefault("tableNum", 1).toString());
        Integer groupNum = Objects.requireNonNull(Integer.parseInt(properties.get("storeNum").toString()));
        SQLMethodInvokeExpr tableMethod = converyToMethodExpr((String) properties.get("tableMethod"));
        SQLMethodInvokeExpr dbMethod = converyToMethodExpr((String) properties.get("dbMethod"));
        String sep = "/";
        String mappingFormat = (String) properties.getOrDefault("mappingFormat",
                String.join(sep, "c${targetIndex}",
                        tableHandler.getSchemaName() + "_${dbIndex}",
                        tableHandler.getTableName() + "_${tableIndex}"));
        List<IndexDataNode> datanodes = new ArrayList<>();
        List<int[]> seq = new ArrayList<>();
        for (int dbIndex = 0; dbIndex < dbNum; dbIndex++) {
            for (int tableIndex = 0; tableIndex < tableNum; tableIndex++) {
                seq.add(new int[]{dbIndex, tableIndex});
            }
        }
        SimpleTemplateEngine templateEngine = new SimpleTemplateEngine();
        Template template = templateEngine.createTemplate(mappingFormat);
        HashMap<String, Object> context = new HashMap<>(properties);

        Map<Key, DataNode> cache = new ConcurrentHashMap<>();
        for (int i = 0; i < seq.size(); i++) {
            int seqIndex = i / groupNum;
            int[] ints = seq.get(i);
            int dbIndex = ints[0];
            int tableIndex = ints[1];
            context.put("targetIndex", String.valueOf(seqIndex));
            context.put("dbIndex", String.valueOf(dbIndex));
            context.put("tableIndex", String.valueOf(tableIndex));
            StringWriter stringWriter = new StringWriter();
            template.make(context).writeTo(stringWriter);
            String[] strings = SplitUtil.split(stringWriter.getBuffer().toString(), sep);

            IndexDataNode backendTableInfo = new IndexDataNode(strings[0], strings[1], strings[2], dbIndex, tableIndex);
            cache.put(new Key(ints[0], ints[1]), backendTableInfo);
            datanodes.add(backendTableInfo);
        }


        ToIntFunction<Object> tableFunction = (o) -> 0;
        Set<String> dbShardingKeys = new HashSet<>();

        ToIntFunction<Object> dbFunction = (o) -> 0;
        Set<String> tableShardingKeys = new HashSet<>();

        if (dbMethod != null) {
            int num = dbNum;
            SQLMethodInvokeExpr methodInvokeExpr = dbMethod;

            if (SQLUtils.nameEquals("HASH", methodInvokeExpr.getMethodName())) {

                dbShardingKeys.add(getShardingKey(methodInvokeExpr));

                dbFunction = o -> singleRemainderHash(num, o);
            }
            if (SQLUtils.nameEquals("MODE_HASH", methodInvokeExpr.getMethodName())) {

                dbShardingKeys.add(getShardingKey(methodInvokeExpr));

                dbFunction = o -> singleModHash(num, o);
            }
            if (SQLUtils.nameEquals("UNI_HASH", methodInvokeExpr.getMethodName())) {

                dbShardingKeys.add(getShardingKey(methodInvokeExpr));

                dbFunction = o -> singleRemainderHash(num, o);
            }
            if (SQLUtils.nameEquals("RIGHT_SHIFT", methodInvokeExpr.getMethodName())) {
                dbShardingKeys.add(getShardingKey(methodInvokeExpr));

                int shift = Integer.parseInt(getShardingKey(methodInvokeExpr, 1));
                dbFunction = o -> singleRightShift(num, shift, o);
            }

            if (SQLUtils.nameEquals("RANGE_HASH", methodInvokeExpr.getMethodName())) {

                dbShardingKeys.add(getShardingKey(methodInvokeExpr));

                dbShardingKeys.add(getShardingKey(methodInvokeExpr, 1));

                int n;
                if (methodInvokeExpr.getArguments().size() > 2) {
                    n = Integer.parseInt(getShardingKey(methodInvokeExpr, 2));
                } else {
                    n = 0;
                }
                dbFunction = o -> singleRangeHash(num, n, o);
            }
            if (SQLUtils.nameEquals("YYYYDD", methodInvokeExpr.getMethodName())) {

                dbShardingKeys.add(getShardingKey(methodInvokeExpr));

                dbFunction = o -> yyyydd(num, o);
            }
            if (SQLUtils.nameEquals("YYYYWEEK", methodInvokeExpr.getMethodName())) {

                dbShardingKeys.add(getShardingKey(methodInvokeExpr));

                dbFunction = o -> yyyyWeek(num, o);
            }
            if ("STR_HASH".equalsIgnoreCase(methodInvokeExpr.getMethodName())) {
                dbShardingKeys.add(getShardingKey(methodInvokeExpr));
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

                tableFunction = value -> strHash(num, startIndex, endIndex, valType, randSeed, value);

            }

        }
        if (tableMethod != null) {
            int num = tableNum;
            SQLMethodInvokeExpr methodInvokeExpr = tableMethod;
            if (SQLUtils.nameEquals("HASH", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableFunction = o -> singleRemainderHash(num, o);
            }
            if (SQLUtils.nameEquals("MOD_HASH", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableFunction = o -> singleModHash(num, o);
            }
            if (SQLUtils.nameEquals("UNI_HASH", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableFunction = o -> singleRemainderHash(num, o);
            }
            if (SQLUtils.nameEquals("RIGHT_SHIFT", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                int shift = Integer.parseInt(getShardingKey(methodInvokeExpr, 1));
                tableFunction = o -> singleRightShift(num, shift, o);
            }
            if (SQLUtils.nameEquals("RANGE_HASH", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableShardingKeys.add(getShardingKey(methodInvokeExpr, 1));
                int n;
                if (methodInvokeExpr.getArguments().size() > 2) {
                    n = Integer.parseInt(getShardingKey(methodInvokeExpr, 2));
                } else {
                    n = 0;
                }
                tableFunction = o -> singleRangeHash(num, n, o);
            }
            if (SQLUtils.nameEquals("YYYYMM", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableFunction = o -> yyyymm(num, o);
            }
            if (SQLUtils.nameEquals("YYYYDD", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableFunction = o -> yyyydd(num, o);
            }
            if (SQLUtils.nameEquals("YYYYWEEK", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableFunction = o -> yyyyWeek(num, o);
            }
            if (SQLUtils.nameEquals("WEEK", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableFunction = o -> week(num, o);
            }
            if (SQLUtils.nameEquals("MMDD", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableFunction = o -> mmdd(num, o);
            }
            if (SQLUtils.nameEquals("DD", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableFunction = o -> dd(num, o);
            }
            if (SQLUtils.nameEquals("MM", methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
                tableFunction = o -> mm(num, o);
            }
            if ("STR_HASH".equalsIgnoreCase(methodInvokeExpr.getMethodName())) {
                tableShardingKeys.add(getShardingKey(methodInvokeExpr));
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

                tableFunction = value -> strHash(num, startIndex, endIndex, valType, randSeed, value);
            }
        }
        if (dbMethod != null && tableMethod != null) {
            if (SQLUtils.nameEquals("HASH", dbMethod.getMethodName())) {
                if (SQLUtils.nameEquals("HASH", tableMethod.getMethodName())) {
                    String tableShardingKey = getShardingKey(tableMethod);
                    String dbShardingKey = getShardingKey(dbMethod);
                    if (tableShardingKey.equalsIgnoreCase(dbShardingKey)) {
                        int total = dbNum * tableNum;
                        tableFunction = o -> {
                            return singleRemainderHash(total, o);
                        };
                        dbFunction = (o) -> {
                            if (o == null) return 0;
                            if (o instanceof Number) {
                                long l = ((Number) o).longValue();
                                long i = l% total / tableNum;
                                if (i < 0) {
                                    throw new IllegalArgumentException();
                                }
                                return (int)i;
                            }
                            if (o instanceof String) {
                                return hashCode((String) o) % total / tableNum;
                            }
                            throw new UnsupportedOperationException();
                        };
                    } else {
                        tableFunction = o -> singleRemainderHash(tableNum, o);
                        dbFunction = o -> singleRemainderHash(dbNum, o);
                    }

                }
            }
            if (SQLUtils.nameEquals("UNI_HASH", dbMethod.getMethodName())) {
                if (SQLUtils.nameEquals("UNI_HASH", tableMethod.getMethodName())) {
                    String tableShardingKey = getShardingKey(tableMethod);
                    String dbShardingKey = getShardingKey(dbMethod);
                    if (tableShardingKey.equalsIgnoreCase(dbShardingKey)) {
                        tableFunction = (o) -> {
                            int total = dbNum * tableNum;
                            if (o instanceof Number) {
                                long intValue = ((Number) o).longValue();

                                long l = (intValue) % dbNum * tableNum
                                        +
                                        (intValue / dbNum) % tableNum;
                                return (int)l;
                            }
                            if (o instanceof String) {
                                return hashCode((String) o) % total / tableNum;
                            }
                            throw new UnsupportedOperationException();
                        };
                        dbFunction = (o) -> singleRemainderHash(dbNum, o);

                    } else {
                        tableFunction = o -> singleRemainderHash(tableNum, o);
                        dbFunction = o -> singleRemainderHash(dbNum, o);
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

                for (String dbShardingKey : dbShardingKeys) {
                    Collection<RangeVariable> rangeVariables = stringCollectionMap.get(dbShardingKey);
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
                for (String tableShardingKey : tableShardingKeys) {
                    Collection<RangeVariable> rangeVariables = stringCollectionMap.get(tableShardingKey);
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
                if (getDbIndex && getTIndex) {
                    DataNode dataNode = cache.get(new Key(dIndex, tIndex));
                    if (dataNode == null) {
                        return (List) datanodes;
                    }
                    return Collections.singletonList(dataNode);
                }
                if (getDbIndex) {
                    List<DataNode> list = new ArrayList<>();
                    for (IndexDataNode i : datanodes) {
                        if (i.getDbIndex() == dIndex) {
                            list.add(i);
                        }
                    }
                    return list;
                }
                if (getTIndex) {
                    List<DataNode> list = new ArrayList<>();
                    for (IndexDataNode i : datanodes) {
                        if (i.getTableIndex() == tIndex) {
                            list.add(i);
                        }
                    }
                    return list;
                }
                return Collections.unmodifiableList(datanodes);
            }
        };
        Set<String> keys = new HashSet<>(dbShardingKeys);
        keys.addAll(tableShardingKeys);

        return new CustomRuleFunction() {
            @Override
            public String name() {
                return MessageFormat.format("dbNum:{0} tableNum:{1} dbMethod:{2} tableMethod:{3}",
                        dbNum, tableNum, dbMethod, tableMethod);
            }

            @Override
            public List<DataNode> calculate(Map<String, Collection<RangeVariable>> values) {
                return Objects.requireNonNull(function.apply(values));
            }

            @Override
            protected void init(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges) {

            }

            @Override
            public boolean isShardingKey(String name) {
                return keys.contains(SQLUtils.normalize(name));
            }
        };
    }

    public static int mm(int num, Object o) {
        if (o == null) return 0;
        Integer mm = null;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            mm = localDate.getMonthValue();
        }
        if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            mm = localDateTime.getMonthValue();
        }
        if (mm == null) {
            throw new UnsupportedOperationException();
        }
        return (mm) % num;
    }

    public static int dd(int num, Object o) {
        if (o == null) return 0;
        Integer day = null;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            day = localDate.getDayOfMonth();
        }
        if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            day = localDateTime.getDayOfMonth();
        }
        if (day == null) {
            throw new UnsupportedOperationException();
        }
        return (day) % num;
    }

    public static int mmdd(int num, Object o) {
        if (o == null) return 0;
        Integer day = null;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            day = localDate.getDayOfYear();
        }
        if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            day = localDateTime.getDayOfYear();
        }
        if (day == null) {
            throw new UnsupportedOperationException();
        }
        return (day) % num;
    }

    public static int strHash(int num, int startIndex, int endIndex, int valType, int randSeed, Object value) {
        if (value == null) value = "null";
        String s = mySubstring(startIndex, endIndex, value.toString());
        if (valType == 0) {
            return hashCode(s, randSeed) % num;
        }
        if (valType == 1) {
            return Integer.parseInt(s) % num;
        }
        throw new UnsupportedOperationException();
    }

    public static int yyyyWeek(int num, Object o) {
        if (o == null) return 0;
        Integer YYYY = null;
        Integer WEEK = null;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            YYYY = localDate.getYear();
            WEEK = localDate.get(WeekFields.ISO.weekOfYear());
        }
        if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            YYYY = localDateTime.getYear();
            WEEK = localDateTime.get(WeekFields.ISO.weekOfYear());
        }
        if (YYYY == null && WEEK == null) {
            throw new UnsupportedOperationException();
        }
        return (YYYY * WEEK + 1) % num;
    }

    public static int week(int num, Object o) {
        if (o == null) return 0;
        Integer day = null;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            day = localDate.getDayOfWeek().getValue();
        }
        if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            day = localDateTime.getDayOfWeek().getValue();
        }
        if (day == null) {
            throw new UnsupportedOperationException();
        }
        return (day) % num;
    }

    public static int yyyydd(int num, Object o) {
        if (o == null) return 0;
        Integer YYYY = null;
        Integer DD = null;
        if (o instanceof String) {
            o = LocalDate.parse((String) o);
        }
        if (o instanceof LocalDate) {
            LocalDate localDate = (LocalDate) o;
            YYYY = localDate.getYear();
            DD = localDate.getDayOfYear();
        }
        if (o instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) o;
            YYYY = localDateTime.getYear();
            DD = localDateTime.getDayOfYear();
        }
        if (YYYY == null && DD == null) {
            throw new UnsupportedOperationException();
        }
        return (YYYY * DD + DD) % num;
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
        return (YYYY * MM + MM) % num;
    }

    public static int singleRangeHash(int num, int n, Object o) {
        if (o == null) o = "null";
        if (o instanceof Number) {
            return (int) (((Number) o).longValue() % num);
        }
        if (o instanceof String) {
            return hashCode(((String) o).substring(n)) % num;
        }
        throw new UnsupportedOperationException();
    }

    public static int singleRightShift(int num, int shift, Object o) {
        if (o == null) o = "null";
        if (o instanceof Number) {
            return (int)( ((Number) o).longValue() >> shift % num);
        }
        if (o instanceof String) {
            return hashCode((String) o) >> shift % num;
        }
        throw new UnsupportedOperationException();
    }

    public static int singleModHash(int num, Object o) {
        if (o == null) o = "null";
        if (o instanceof Number) {
            return (int)Math.floorMod(((Number) o).longValue(), num);
        }
        if (o instanceof String) {
            return Math.floorMod(hashCode((String) o), num);
        }
        throw new UnsupportedOperationException();
    }

    public static int singleRemainderHash(int num, Object o) {
        if (o == null) o = "null";
        if (o instanceof Number) {
            long l = ((Number) o).longValue();
            long l1 = l % num;
            return (int)l1;
        }
        if (o instanceof String) {
            return hashCode((String) o) % num;
        }
        throw new UnsupportedOperationException();
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

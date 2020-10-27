package io.mycat.router.function;

import com.alibaba.fastsql.sql.parser.SQLExprParser;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
import groovy.text.SimpleTemplateEngine;
import groovy.text.Template;
import io.mycat.BackendTableInfo;
import io.mycat.DataNode;
import io.mycat.RangeVariable;
import io.mycat.RangeVariableType;
import io.mycat.config.ShardingFuntion;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.SplitUtil;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.Nullable;

import java.io.StringWriter;
import java.util.*;
import java.util.function.Function;

public class AutoFunctionFactory {


    public static final CustomRuleFunction getFunction(ShardingTableHandler tableHandler, ShardingFuntion config) {
        Info info = Objects.requireNonNull(getTableFunction(tableHandler,config));
        return new CustomRuleFunction() {
            @Override
            public String name() {
                return null;
            }

            @Override
            public List<DataNode> calculate(Map<String, Collection<RangeVariable>> values) {
                Function<Map<String, Collection<RangeVariable>>, List<DataNode>> function = info.getFunction();
                return function.apply(values);
            }

            @Override
            protected void init(ShardingTableHandler tableHandler, Map<String, String> properties, Map<String, String> ranges) {

            }

            @Override
            public boolean isShardingKey(String name) {
                return info.isShardingKey(name);
            }
        };
    }

    @EqualsAndHashCode
    @AllArgsConstructor
    @Getter
    static class Key {
        int dbIndex;
        int tableIndex;
    }

    static class Info {
        private Set<String> columns = new HashSet<>();
        private Function<Map<String, Collection<RangeVariable>>, List<DataNode>> function;
        private List<DataNode> dataNodes;

        public void addColumnName(String columnName) {
            columns.add(columnName);
        }

        public void setFunction(Function<Map<String, Collection<RangeVariable>>, List<DataNode>> function) {
            this.function = function;
        }

        public boolean isShardingKey(String name) {
            return columns.contains(name);
        }

        public Set<String> getColumns() {
            return columns;
        }

        public Function<Map<String, Collection<RangeVariable>>, List<DataNode>> getFunction() {
            return function;
        }

        public List<DataNode> getDataNodes() {
            return dataNodes;
        }

        public void setDataNodes(List<DataNode> dataNodes) {
            this.dataNodes = dataNodes;
        }
    }

    @SneakyThrows
    private static final Info getTableFunction(ShardingTableHandler tableHandler, ShardingFuntion config) {
        Info info = new Info();
        Map<String, Object> properties = config.getProperties();
        Integer targetNum = Integer.parseInt(properties.getOrDefault("targetNum", 1).toString());
        Integer dbNum = Integer.parseInt(properties.getOrDefault("dbNum", 8).toString());
        int tableNum = Integer.parseInt(properties.getOrDefault("tableNum", 1).toString());
        Integer groupNum =  (dbNum * tableNum);
        SQLMethodInvokeExpr tableMethod = converyToMethodExpr((String) properties.get("tableMethod"));
        SQLMethodInvokeExpr dbMethod = converyToMethodExpr((String) properties.get("dbMethod"));
        String mappingFormat = (String) properties.getOrDefault("mappingFormat",
                "c${targetIndex}." +
                        tableHandler.getSchemaName()+"_${dbIndex}." +tableHandler.getTableName()+"_"+
                          "${tableIndex}");
        List<DataNode> datanodes = new ArrayList<>();
        List<int[]> seq = new ArrayList<>();
        for (int dbIndex = 0; dbIndex < dbNum; dbIndex++) {
            for (int tableIndex = 0; tableIndex < tableNum; tableIndex++) {
                seq.add(new int[]{dbIndex, tableIndex});
            }
        }
        SimpleTemplateEngine templateEngine = new SimpleTemplateEngine();
        Template template = templateEngine.createTemplate(mappingFormat);
        HashMap<String, Object> context = new HashMap<>(properties);

        HashMap<Key, DataNode> cache = new HashMap<>();
        for (int i = 0; i < seq.size(); i++) {
            int seqIndex = i / groupNum;
            int[] ints = seq.get(i);
            context.put("targetIndex", String.valueOf(seqIndex));
            context.put("dbIndex", String.valueOf(ints[0]));
            context.put("tableIndex", String.valueOf(ints[1]));
            StringWriter stringWriter = new StringWriter();
            template.make(context).writeTo(stringWriter);
            String[] strings = SplitUtil.split(stringWriter.getBuffer().toString(),'.') ;

            BackendTableInfo backendTableInfo = new BackendTableInfo(strings[0], strings[1], strings[2]);
            cache.put(new Key(ints[0], ints[1]), backendTableInfo);
            datanodes.add(backendTableInfo);
        }
        info.setDataNodes(datanodes);


        if (dbMethod != null && tableMethod != null) {
            if (SQLUtils.nameEquals("HASH", tableMethod.getMethodName())
                    && SQLUtils.nameEquals(tableMethod.getMethodName(), dbMethod.getMethodName())) {
                String columnName = SQLUtils.normalize(tableMethod.getArguments().get(0).toString());
                info.addColumnName(columnName);
                int totalNum = dbNum * tableNum;
                info.setFunction(getter -> {
                    Collection<RangeVariable> rangeVariables = getter.get(columnName);
                    if (rangeVariables.size() == 1) {
                        for (RangeVariable rangeVariable : rangeVariables) {
                            if (rangeVariable.getOperator() == RangeVariableType.EQUAL) {
                                int hashCode = rangeVariable.getValue().hashCode();
                                return Collections.singletonList(
                                        cache.get(new Key(hashCode % dbNum, hashCode % totalNum))
                                );
                            }
                        }
                    }
                    return datanodes;
                });
                return info;
            }
        } else if (dbMethod != null && tableMethod == null) {
            SQLMethodInvokeExpr methodExpr = dbMethod;
            String columnName = SQLUtils.normalize(methodExpr.getArguments().get(0).toString());
            int num = dbNum;
            info.addColumnName(columnName);
            Function<Map<String, Collection<RangeVariable>>, int[]> mFunction = simpleFunction(methodExpr, columnName, num);
            info.setFunction(mFunction.andThen(i -> {
                if (i == null) {
                    return datanodes;
                }
                if (i.length == 1) {
                    return Collections.singletonList(cache.get(new Key(i[0], 0)));
                }
                return datanodes;
            }));
            return info;
        } else if (dbMethod == null && tableMethod != null) {
            SQLMethodInvokeExpr methodExpr = tableMethod;
            String columnName = SQLUtils.normalize(methodExpr.getArguments().get(0).toString());
            int num = tableNum;
            info.addColumnName(columnName);
            Function<Map<String, Collection<RangeVariable>>, int[]> mFunction = simpleFunction(methodExpr, columnName, num);
            info.setFunction(mFunction.andThen(i -> {
                if (i == null) {
                    return datanodes;
                }
                if (i.length == 1) {
                    return Collections.singletonList(cache.get(new Key(0, i[0])));
                }
                return datanodes;
            }));
            return info;
        }
        throw new UnsupportedOperationException();
    }

    @Nullable
    private static Function<Map<String, Collection<RangeVariable>>, int[]> simpleFunction(SQLMethodInvokeExpr methodExpr,
                                                                                          String column,
                                                                                          int num) {
        String methodName = SQLUtils.normalize(methodExpr.getMethodName());
        Function<Map<String, Collection<RangeVariable>>, int[]> mFunction = null;
        if ("RANGE_HASH".equalsIgnoreCase(methodName)) {
            mFunction = rangeHash(methodExpr, num);
        } else if ("HASH".equalsIgnoreCase(methodName)) {
            mFunction = getter -> {
                Collection<RangeVariable> rangeVariables = getter.get(column);
                if (rangeVariables != null && rangeVariables.size() == 1) {
                    for (RangeVariable rangeVariable : rangeVariables) {
                        if (rangeVariable.getOperator() == RangeVariableType.EQUAL) {
                            Object value = rangeVariable.getValue();
                            return new int[]{value.hashCode() % num};
                        }
                    }
                }
                return null;
            };
        }
        return mFunction;
    }

    private static SQLMethodInvokeExpr converyToMethodExpr(String methodExpr) {
        if (methodExpr == null) return null;
        SQLExprParser sqlExprParser = new SQLExprParser(methodExpr);
        return (SQLMethodInvokeExpr) sqlExprParser.expr();
    }

    private static Function<Map<String, Collection<RangeVariable>>, int[]> rangeHash(SQLMethodInvokeExpr expr, int shardingNum) {
        List<SQLExpr> arguments = expr.getArguments();
        String column1 = SQLUtils.normalize(arguments.get(0).toString());
        String column2 = SQLUtils.normalize(arguments.get(1).toString());
        int n = Integer.parseInt(arguments.get(2).toString());
        return getter -> {
            Collection<RangeVariable> o = Optional.ofNullable(getter.get(column1))
                    .orElse(Objects.requireNonNull(getter.get(column2)));
            if (o != null && o.size() == 1) {
                for (RangeVariable rangeVariable : o) {
                    if (rangeVariable.getOperator() == RangeVariableType.EQUAL) {
                        String tmp = o.toString().substring(n);
                        return new int[]{tmp.hashCode() % shardingNum};
                    }
                }
            }
            return null;
        };
    }

//    public static final AutoFunction getDbFunction(
//            String simpleName,
//            Class... argTypes) {
//        return getAutoFunction(simpleName, dbFunctionMap, argTypes);
//    }
//
//    public static final AutoFunction getTableFunction(
//            String simpleName,
//            Class... argTypes) {
//        return getAutoFunction(simpleName, tableFunctionMap, argTypes);
//    }
//    public static final AutoFunction getTableFunction(
//            String simpleName,
//            Object... argTypes) {
//        return getAutoFunction(simpleName, tableFunctionMap, argTypes);
//    }
//    private static AutoFunction getAutoFunction(String simpleName, Map<String, List<AutoFunction>> map, Class[] argTypes) {
//        List<AutoFunction> functions = map.get(SQLUtils.normalize(simpleName).toUpperCase());
//        return functions.stream().filter(autoFunction -> {
//            Class[] params = autoFunction.getParams();
//            if (params.length == argTypes.length) {
//                for (int i = 0; i < params.length; i++) {
//                    if (!params[i].isAssignableFrom(argTypes[i])) {
//                        return false;
//                    }
//                }
//                return true;
//            } else {
//                return false;
//            }
//        }).findFirst().orElse(null);
//    }

    enum Type {
        TABLE,
        DATABASE,
        BOTH
    }

    interface AutoFunction {
        String getName();

        Class[] getParams();

        Type getType();
    }
}

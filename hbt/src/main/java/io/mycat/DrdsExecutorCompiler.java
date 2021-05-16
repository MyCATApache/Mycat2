package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.MySQLType;
import io.mycat.calcite.*;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.logical.MycatViewDataNodeMapping;
import io.mycat.calcite.logical.MycatViewSqlString;
import io.mycat.calcite.logical.ViewInfo;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatMergeSort;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.plan.PlanImplementor;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.spm.ParamHolder;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.PlanImpl;
import io.mycat.calcite.spm.QueryPlanner;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.util.NameMap;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.MycatTransientSQLTableScanMappingFunction;
import io.mycat.vertx.ShardingTableDataNodeMapping;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.curator.shaded.com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DrdsExecutorCompiler {
    private final static Logger log = LoggerFactory.getLogger(DrdsExecutorCompiler.class);

    public static SqlTypeName getObjectType(Object value) {
        if (value == null) {
            return (SqlTypeName.NULL);
        } else {
            Class<?> aClass = value.getClass();
            MySQLType[] mySQLTypes = MySQLType.values();
            for (MySQLType e : mySQLTypes) {
                if (e.getJavaClass() == aClass) {
                    return (SqlTypeName.getNameForJdbcType(e.getJdbcType()));
                }
                if (Integer.class == aClass) {
                    return SqlTypeName.INTEGER;
                }
                if (byte[].class == aClass) {
                    return SqlTypeName.BINARY;
                }
            }
            throw new IllegalArgumentException("unknown type :" + aClass);
        }
    }

    private static List<Object> normalize(List<SqlTypeName> targetTypes, List<Object> params) {
        int size = targetTypes.size();
        if (targetTypes.isEmpty()) {
            return Collections.emptyList();
        }
        if (params.get(0) instanceof List) {
            return params;//insert or update sql
        }
        List<Object> resParams = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Object param = params.get(i);
            SqlTypeName targetTypeName = targetTypes.get(i);
            SqlTypeName curTypeName = getObjectType(param);
            Object curParam = params.get(i);
            if (targetTypeName == curTypeName || curParam == null) {
                resParams.add(curParam);
            } else {
                switch (targetTypeName) {
                    case BOOLEAN:
                        if (curParam instanceof Boolean) {
                            curParam = curParam;
                        } else if (curParam instanceof String) {
                            curParam = Boolean.parseBoolean((String) curParam);
                        } else if (curParam instanceof Number) {
                            long l = ((Number) curParam).longValue();
                            curParam = (l == -1 || l > 0);
                        }
                        throw new UnsupportedOperationException();
                    case TINYINT:
                        if (curParam instanceof Number) {
                            curParam = ((Number) curParam).byteValue();
                        } else if (curParam instanceof String) {
                            curParam = Byte.parseByte((String) curParam);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case SMALLINT:
                        if (curParam instanceof Number) {
                            curParam = ((Number) curParam).shortValue();
                        } else if (curParam instanceof String) {
                            curParam = Short.parseShort((String) curParam);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case INTEGER:
                        if (curParam instanceof Number) {
                            curParam = ((Number) curParam).intValue();
                        } else if (curParam instanceof String) {
                            curParam = Integer.parseInt((String) curParam);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case BIGINT:
                        if (curParam instanceof Number) {
                            curParam = ((Number) curParam).longValue();
                        } else if (curParam instanceof String) {
                            curParam = Long.parseLong((String) curParam);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case DECIMAL:
                        if (curParam instanceof Number) {
                            curParam = new BigDecimal(curParam.toString());
                        } else if (curParam instanceof String) {
                            curParam = new BigDecimal(curParam.toString());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case FLOAT:
                        if (curParam instanceof Float) {
                            curParam = ((Number) curParam).floatValue();
                        } else if (curParam instanceof String) {
                            curParam = Float.parseFloat(curParam.toString());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case REAL:
                    case DOUBLE:
                        if (curParam instanceof Number) {
                            curParam = ((Number) curParam).doubleValue();
                        } else if (curParam instanceof String) {
                            curParam = Double.parseDouble(curParam.toString());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case DATE:
                        if (curParam instanceof Date) {
                            curParam = new java.sql.Date(((Date) curParam).getTime()).toLocalDate();
                        } else if (curParam instanceof String) {
                            curParam = java.sql.Date.valueOf((String) curParam);
                        } else if (curParam instanceof LocalDate) {
                            curParam = curParam;
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case TIME_WITH_LOCAL_TIME_ZONE:
                    case TIME:
                        if (curParam instanceof Duration) {
                            curParam = curParam;
                        } else {
                            curParam = curParam.toString();
                        }
                        curParam = MycatTimeUtil.timeStringToTimeDuration(curParam.toString());
                        break;
                    case TIMESTAMP:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        if (curParam instanceof Timestamp) {
                            curParam = ((Timestamp) curParam).toLocalDateTime();
                        } else if (curParam instanceof String) {
                            curParam = Timestamp.valueOf((String) curParam).toLocalDateTime();
                        }
                    {
                        curParam = curParam;
                    }
                    break;
                    case CHAR:
                        curParam = curParam.toString();
                        break;
                    case VARCHAR:
                        curParam = curParam.toString();
                        break;
                    case BINARY:
                    case VARBINARY:
                        if (curParam instanceof byte[]) {
                            curParam = new ByteString((byte[]) curParam);
                        } else {
                            curParam = curParam;
                        }
                        break;
                    case NULL:
                        curParam = null;
                        break;
                    case ANY:
                    case SYMBOL:
                    case MULTISET:
                    case ARRAY:
                    case MAP:
                    case DISTINCT:
                    case STRUCTURED:
                    case ROW:
                    case OTHER:
                    case CURSOR:
                    case COLUMN_LIST:
                    case DYNAMIC_STAR:
                    case GEOMETRY:
                    case SARG:
                        curParam = curParam.toString();
                        break;
                    case INTERVAL_YEAR:
                    case INTERVAL_YEAR_MONTH:
                    case INTERVAL_MONTH:
                    case INTERVAL_DAY:
                    case INTERVAL_DAY_HOUR:
                    case INTERVAL_DAY_MINUTE:
                    case INTERVAL_DAY_SECOND:
                    case INTERVAL_HOUR:
                    case INTERVAL_HOUR_MINUTE:
                    case INTERVAL_HOUR_SECOND:
                    case INTERVAL_MINUTE:
                    case INTERVAL_MINUTE_SECOND:
                    case INTERVAL_SECOND:
                    case INTERVAL_MICROSECOND:
                    case INTERVAL_WEEK:
                    case INTERVAL_QUARTER:
                    case INTERVAL_SECOND_MICROSECOND:
                    case INTERVAL_MINUTE_MICROSECOND:
                    case INTERVAL_HOUR_MICROSECOND:
                    case INTERVAL_DAY_MICROSECOND:
                        throw new UnsupportedOperationException();
                }
                resParams.add(curParam);
            }
        }
        return resParams;
    }

    public static List<SqlTypeName> getSqlTypeNames(List<Object> params) {
        ArrayList<SqlTypeName> list = new ArrayList<>();
        for (Object param : params) {
            if (param == null) {
                list.add(SqlTypeName.NULL);
            } else {
                Class<?> aClass = param.getClass();
                SqlTypeName sqlTypeName = null;
                MySQLType[] mySQLTypes = MySQLType.values();
                for (MySQLType value : mySQLTypes) {
                    if (value.getJavaClass() == aClass) {
                        sqlTypeName = (SqlTypeName.getNameForJdbcType(value.getJdbcType()));
                        break;
                    }
                    if (Integer.class == aClass) {
                        sqlTypeName = SqlTypeName.INTEGER;
                        break;
                    }
                    if (byte[].class == aClass) {
                        sqlTypeName = SqlTypeName.BINARY;
                        break;
                    }
                }
                list.add(Objects.requireNonNull(sqlTypeName, () -> "unknown type :" + param.getClass()));
            }
        }
        return list;
    }

    @NotNull
    @SneakyThrows
    public static CodeExecuterContext getCodeExecuterContext(MycatRel relNode, boolean forUpdate, DrdsSql drdsSql) {
        HashMap<String, Object> varContext = new HashMap<>(2);
        StreamMycatEnumerableRelImplementor mycatEnumerableRelImplementor = new StreamMycatEnumerableRelImplementor(varContext);
        RelContext relContext = new RelContext(relNode, forUpdate);
        relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(RelNode other) {
                if (other instanceof MycatView) {
                    Rel rel = relContext.nodes.computeIfAbsent(other.getDigest(), s -> new Rel());
                    rel.count += 1;
                    MycatView mycatView = (MycatView) other;
                    visitMycatView(rel, mycatView);
                } else if (other instanceof MycatMergeSort) {
                    Rel rel = relContext.nodes.computeIfAbsent(other.getDigest(), s -> new Rel());
                    rel.count += 1;
                    MycatMergeSort mycatMergeSort = (MycatMergeSort) other;
                    visitMycatView(rel, (MycatView) mycatMergeSort.child);
                } else if (other instanceof MycatTransientSQLTableScan) {
                    Rel rel = relContext.nodes.computeIfAbsent(other.getDigest(), s -> new Rel());
                    rel.count += 1;
                    MycatTransientSQLTableScan transientSQLTableScan = (MycatTransientSQLTableScan) other;
                    visitTableScan(rel, transientSQLTableScan);
                }
                return super.visit(other);
            }

            private void visitTableScan(Rel rel, MycatTransientSQLTableScan transientSQLTableScan) {
                String sql = transientSQLTableScan.getSql();
                String targetName = transientSQLTableScan.getTargetName();
                rel.dataNodeMapping = new MycatTransientSQLTableScanMappingFunction(targetName, sql);
                rel.columnInfo = new CopyMycatRowMetaData(new CalciteRowMetaData(transientSQLTableScan.getRowType().getFieldList()));
            }

            private void visitMycatView(Rel rel, MycatView mycatView) {
                MycatViewDataNodeMapping mycatViewDataNodeMapping = generateMycatView(mycatView, drdsSql);
                SqlNode sqlTemplate = mycatView.getSQLTemplate(forUpdate);
                rel.dataNodeMapping = new ShardingTableDataNodeMapping(mycatViewDataNodeMapping, sqlTemplate);
                rel.columnInfo = new CopyMycatRowMetaData(new CalciteRowMetaData(mycatView.getRowType().getFieldList()));
            }
        });

        ClassDeclaration classDeclaration = mycatEnumerableRelImplementor.implementHybridRoot(relNode, EnumerableRel.Prefer.ARRAY);
        String code = Expressions.toString(classDeclaration.memberDeclarations, "\n", false);
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------code----------------------------------------");
            log.debug(code);
        }
        CodeContext codeContext = new CodeContext(classDeclaration.name, code);

        CodeExecuterContext executerContext = CodeExecuterContext.of(relContext, varContext, codeContext);
        return executerContext;
    }

    @Nullable
    private static MycatViewDataNodeMapping generateMycatView(MycatView mycatView, DrdsSql drdsSql) {
        final MycatViewDataNodeMapping orginalMycatViewDataNodeMapping = mycatView.getMycatViewDataNodeMapping();
        MycatViewDataNodeMapping resultMycatViewDataNodeMapping = orginalMycatViewDataNodeMapping;
        for (MycatHint mycatHint : Optional.ofNullable(drdsSql.getHints()).orElse(Collections.emptyList())) {
            for (MycatHint.Function function : mycatHint.getFunctions()) {
                if ("SCAN".equals(function.getName())) {
                    List<MycatHint.Argument> arguments = function.getArguments();
                    Map<String, List<String>> collect = arguments.stream().collect(Collectors.toMap(m -> SQLUtils.toSQLString(m.getName()), v -> Arrays.asList(SQLUtils.toSQLString(v.getValue()).split(","))));
                    NameMap<List<String>> nameMap = NameMap.immutableCopyOf(collect);
                    List<String> condition = Optional.ofNullable(nameMap.get("CONDITION", false)).orElse(Collections.emptyList());
                    List<String> logicalTables = Optional.ofNullable(nameMap.get("LOGICAL_TABLE", false)).orElse(Collections.emptyList());
                    List<List<String>> physicalTables = Optional.ofNullable(nameMap.get("PHYSICAL_TABLE", false)).orElse(Collections.emptyList()).stream().map(i -> Arrays.asList(i.split(","))).collect(Collectors.toList());
                    List<String> targets = Optional.ofNullable(nameMap.get("TARGET", false)).orElse(Collections.emptyList());
                    Map<String, List<DataNode>> logicalPhysicalMap = new HashMap<>();
                    int physicalTableCount = 0;
                    if (!physicalTables.isEmpty()) {
                        int size = logicalTables.size();
                        for (int i = 0; i < size; i++) {
                            String alias = logicalTables.get(i);
                            List<DataNode> phyTableList = physicalTables.get(i).stream().map(s -> {
                                String[] s1 = s.split("_");
                                if (s1.length >= 3) {
                                    return new BackendTableInfo(s1[0], s1[1], s1[2]);
                                }
                                if (s1.length == 2) {
                                    return new BackendTableInfo(null, s1[0], s1[1]);
                                }
                                return new BackendTableInfo(null, null, s1[0]);
                            }).collect(Collectors.toList());
                            physicalTableCount += phyTableList.size();
                            logicalPhysicalMap.put(alias, phyTableList);
                        }
                    }

                    if (!logicalTables.isEmpty() && !condition.isEmpty()) {
                        String where = condition.get(0);
                        String sql;
                        ViewInfo viewInfo = orginalMycatViewDataNodeMapping.viewInfo();
                        Map<String, MycatLogicTable> aliasTableMap = viewInfo.getAliasTableMap();
                        StringJoiner sb = new StringJoiner(",");
                        for (Map.Entry<String, MycatLogicTable> e : aliasTableMap.entrySet()) {
                            TableHandler table = e.getValue().getTable();
                            String schemaName = table.getSchemaName();
                            String tableName = table.getTableName();
                            sb.add(schemaName + "." + tableName);
                        }

                        sql = "select *  from " + sb.toString() + " where " + where;
                        DrdsSqlWithParams hintSql = DrdsRunnerHelper.preParse(sql, null);

                        QueryPlanner planner = MetaClusterCurrent.wrapper(QueryPlanner.class);
                        MycatViewDataNodeMapping hintMycatViewDataNodeMapping = null;
                        try {
                            ParamHolder.CURRENT_THREAD_LOCAL.set(hintSql.getParams());
                            CodeExecuterContext codeExecuterContext = planner.innerComputeMinCostCodeExecuterContext(hintSql);
                            MycatView mycatRel = (MycatView) codeExecuterContext.getMycatRel();
                            hintMycatViewDataNodeMapping = mycatRel.getMycatViewDataNodeMapping();
                        } catch (Exception e) {
                            log.error("", e);
                        } finally {
                            ParamHolder.CURRENT_THREAD_LOCAL.set(null);
                        }
                        if (hintMycatViewDataNodeMapping != null) {
                            final MycatViewDataNodeMapping finalHintMycatViewDataNodeMapping = hintMycatViewDataNodeMapping;
                            final int finalPhysicalTableCount = physicalTableCount;
                            resultMycatViewDataNodeMapping = new MycatViewDataNodeMapping() {
                                @Override
                                public boolean containsOrder() {
                                    return orginalMycatViewDataNodeMapping.containsOrder();
                                }

                                @Override
                                public Distribution.Type getType() {
                                    return orginalMycatViewDataNodeMapping.getType();
                                }

                                @Override
                                public Distribution distribution() {
                                    return orginalMycatViewDataNodeMapping.distribution();
                                }

                                @Override
                                public ViewInfo viewInfo() {
                                    return orginalMycatViewDataNodeMapping.viewInfo();
                                }

                                @Override
                                public Stream<Map<String, DataNode>> apply(List<Object> objects) {
                                    List<Map<String, DataNode>> hintList = finalHintMycatViewDataNodeMapping.apply(objects).collect(Collectors.toList());
                                    List<Map<String, DataNode>> originalList = orginalMycatViewDataNodeMapping.apply(objects).collect(Collectors.toList());


                                    int size = Math.min(hintList.size(), originalList.size());

                                    if (!physicalTables.isEmpty()) {
                                        size = Math.min(finalPhysicalTableCount, size);
                                    }
                                    List<Map<String, DataNode>> resultSetList = new ArrayList<>(size);
                                    for (int i = 0; i < size; i++) {
                                        Map<String, DataNode> hintMap = hintList.get(i);
                                        Map<String, DataNode> originalMap = originalList.get(i);
                                        Map<String, DataNode> resultMap = new HashMap<>(originalMap.size());

                                        resultMap.putAll(originalMap);
                                        resultMap.putAll(hintMap);

                                        if (!physicalTables.isEmpty()) {
                                            for (String s : new ArrayList<>(resultMap.keySet())) {
                                                DataNode originalDataNode = resultMap.get(s);
                                                if (logicalPhysicalMap.containsKey(s)) {
                                                    List<DataNode> list = logicalPhysicalMap.get(s);
                                                    DataNode dataNode = list.get(0);
                                                    String targetName = dataNode.getTargetName();
                                                    String schema = dataNode.getSchema();
                                                    String table = dataNode.getTable();

                                                    if (targetName==null){
                                                        targetName = originalDataNode.getTargetName();
                                                    }
                                                    if (schema == null){
                                                        schema = originalDataNode.getSchema();
                                                    }
                                                    resultMap.put(s,new BackendTableInfo(targetName,schema,table));
                                                }
                                            }

                                        }
                                        resultSetList.add(resultMap);
                                    }
                                    if (!targets.isEmpty()) {
                                        Map<Boolean, List<String>> filterMap = targets.stream().collect(Collectors.partitioningBy(k -> Character.isDigit(k.charAt(0))));
                                        Set<Integer> indexFilter = filterMap.get(Boolean.TRUE).stream().map(i -> Integer.parseInt(i)).collect(Collectors.toSet());
                                        List<Map<String, DataNode>> newResultSetList = new ArrayList<>(size);
                                        for (int i = 0; i < resultSetList.size(); i++) {
                                            if (indexFilter.contains(i)){
                                                newResultSetList.add(resultSetList.get(i));
                                            }
                                        }
                                        resultSetList = newResultSetList;
                                        List<String> nameFilter = filterMap.get(Boolean.FALSE);
                                        resultSetList = resultSetList.stream().filter(stringDataNodeMap -> {
                                            for (DataNode value : stringDataNodeMap.values()) {
                                                return nameFilter.contains(value.getTargetName());
                                            }
                                            return false;
                                        }).collect(Collectors.toList());
                                    }
                                    return resultSetList.stream();
                                }
                            };
                        }
                    }
                    break;
                }
            }
        }

        return resultMycatViewDataNodeMapping;
    }
//
//    @SneakyThrows
//    public Future<Void> runOnDrds(MycatDataContext dataContext,
//                                  DrdsSqlWithParams drdsSqlWithParams, Response response) {
//        DrdsSql drdsSql = this.preParse(statement);
//        Plan plan = getPlan(dataContext, drdsSql);
//        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
//        List<Object> params = drdsSqlWithParams.getParams();
//        PlanImplementor planImplementor = new ObservablePlanImplementorImpl(
//                transactionSession,
//                dataContext, params, response);
//        return impl(plan, planImplementor);
//    }

    @SneakyThrows
    public static Plan convertToExecuter(MycatRel mycatRel) {
        if (mycatRel instanceof MycatUpdateRel) {
            return new PlanImpl((MycatUpdateRel) mycatRel);
        } else if (mycatRel instanceof MycatInsertRel) {
            return new PlanImpl((MycatInsertRel) mycatRel);
        }
//        CodeExecuterContext codeExecuterContext = getCodeExecuterContext(Objects.requireNonNull(mycatRel), forUpdate);
//        return new PlanImpl(mycatRel, codeExecuterContext, forUpdate);
        return null;
    }


    //    public Future<Void> runHbtOnDrds(MycatDataContext dataContext, String statement, Response response) {
//        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
//        List<Object> params = Collections.emptyList();
//        PlanImplementor planImplementor = new ObservablePlanImplementorImpl(
//                transactionSession,
//                dataContext, params, response);
//        Plan hbtPlan = getHbtPlan(statement);
//        return planImplementor.executeQuery(hbtPlan);
//    }
    private Future<Void> impl(Plan plan, PlanImplementor planImplementor) {
        switch (plan.getType()) {
            case PHYSICAL:
                return planImplementor.executeQuery(plan);
            case UPDATE:
                return planImplementor.executeUpdate(Objects.requireNonNull(plan));
            case INSERT:
                return planImplementor.executeInsert(Objects.requireNonNull(plan));
            default: {
                return VertxUtil.newFailPromise(new MycatException(MySQLErrorCode.ER_NOT_SUPPORTED_YET, "不支持的执行计划"));
            }
        }
    }

}

package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.MycatHint;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.logical.MycatViewDataNodeMapping;
import io.mycat.calcite.spm.ParamHolder;
import io.mycat.calcite.spm.QueryPlanner;
import io.mycat.util.NameMap;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.curator.shaded.com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


public class DrdsSqlWithParams extends DrdsSql {
    private final List<Object> params;
    private final List<String> aliasList;
    private final static Logger log = LoggerFactory.getLogger(DrdsSqlWithParams.class);

    public DrdsSqlWithParams(String parameterizedSqlStatement,
                             List<Object> params,
                             boolean complex,
                             List<SqlTypeName> typeNames,
                             List<String> aliasList,
                             List<MycatHint> hints) {
        super(parameterizedSqlStatement, complex, typeNames, hints);
        this.params = params;
        this.aliasList = aliasList;
    }

    public List<Object> getParams() {
        return params;
    }

    public List<String> getAliasList() {
        return aliasList;
    }

    public Optional<List<Map<String, DataNode>>> getHintDataMapping() {
        for (MycatHint hint : this.getHints()) {
            for (MycatHint.Function hintFunction : hint.getFunctions()) {
                if ("SCAN".equalsIgnoreCase(hintFunction.getName())) {
                    List<MycatHint.Argument> arguments = hintFunction.getArguments();
                    Map<String, List<String>> collect = arguments.stream().collect(Collectors.toMap(m -> SQLUtils.toSQLString(m.getName()), v -> Arrays.asList(SQLUtils.toSQLString(v.getValue()).split(","))));
                    NameMap<List<String>> nameMap = NameMap.immutableCopyOf(collect);
                    List<String> condition = Optional.ofNullable(nameMap.get("CONDITION", false)).orElse(Collections.emptyList());
                    List<String> logicalTables = Optional.ofNullable(nameMap.get("TABLE", false)).orElse(Collections.emptyList());
                    List<List<String>> physicalTables = Optional.ofNullable(nameMap.get("DATANODE", false)).orElse(Collections.emptyList()).stream().map(i -> Arrays.asList(i.split(","))).collect(Collectors.toList());
                    List<String> targets = Optional.ofNullable(nameMap.get("TARGET", false)).orElse(Collections.emptyList());

                    MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                    SQLStatement parameterizedStatement = this.getParameterizedStatement();

                    Detector detector = new Detector();
                    parameterizedStatement.accept(detector);
                    if (!detector.isSharding) {
                        Collection<DataNode> dataNodes = Collections.emptyList();
                        Collection<DataNode> newDataNodes = Collections.emptyList();
                        if (!logicalTables.isEmpty()) {
                            if (!condition.isEmpty()) {
                                String sql = "select * from " + logicalTables.get(0) + "where " + condition;
                                QueryPlanner queryPlanner = MetaClusterCurrent.wrapper(QueryPlanner.class);
                                DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sql, null);
                                MycatView mycatView = (MycatView) queryPlanner.innerComputeMinCostCodeExecuterContext(drdsSqlWithParams).getMycatRel();
                                MycatViewDataNodeMapping mycatViewDataNodeMapping = mycatView.getMycatViewDataNodeMapping();
                                List<Map<String, DataNode>> list = mycatViewDataNodeMapping.apply(drdsSqlWithParams.getParams()).collect(Collectors.toList());
                                newDataNodes = dataNodes = list.get(0).values();
                            } else if (!physicalTables.isEmpty()) {
                                newDataNodes = physicalTables.get(0).stream().map(dataNodeParser()).collect(Collectors.toList());
                            }
                            if (!targets.isEmpty()) {
                                newDataNodes = dataNodes.stream().filter(dataNode -> {
                                    return targets.contains(dataNode.getTargetName());
                                }).collect(Collectors.toList());
                            }
                            Collection<DataNode> finalNewDataNodes = newDataNodes;
                            String uniqueName = detector.tableHandlerMap.values().iterator().next().getUniqueName();
                            List<Map<String, DataNode>> res = new ArrayList<>(newDataNodes.size());
                            for (DataNode finalNewDataNode : finalNewDataNodes) {
                                res.add(ImmutableMap.of(uniqueName, finalNewDataNode));
                            }
                            return Optional.of(res);
                        }
                    } else {
                        Map<String, List<DataNode>> logicalPhysicalMap = new HashMap<>();
                        int physicalTableCount = 0;
                        if (!physicalTables.isEmpty()) {
                            int size = logicalTables.size();

                            for (int i = 0; i < size; i++) {
                                String alias = logicalTables.get(i);
                                List<DataNode> phyTableList = physicalTables.get(i).stream().map(dataNodeParser()).collect(Collectors.toList());
                                physicalTableCount += phyTableList.size();
                                logicalPhysicalMap.put(alias, phyTableList);
                            }
                        }

                        if (!logicalTables.isEmpty() && !condition.isEmpty()) {
                            String where = condition.get(0);
                            String sql;
                            Map<String, TableHandler> aliasTableMap = detector.tableHandlerMap;
                            StringJoiner sb = new StringJoiner(",");
                            for (Map.Entry<String, TableHandler> e : aliasTableMap.entrySet()) {
                                TableHandler table = e.getValue();
                                String schemaName = table.getSchemaName();
                                String tableName = table.getTableName();
                                sb.add(schemaName + "." + tableName);
                            }

                            sql = "select *  from " + sb.toString() + " where " + where;
                            DrdsSqlWithParams hintSql = DrdsRunnerHelper.preParse(sql, null);

                            QueryPlanner planner = MetaClusterCurrent.wrapper(QueryPlanner.class);
                            try {
                                ParamHolder.CURRENT_THREAD_LOCAL.set(hintSql.getParams());
                                CodeExecuterContext codeExecuterContext = planner.innerComputeMinCostCodeExecuterContext(hintSql);
                                MycatView mycatRel = (MycatView) codeExecuterContext.getMycatRel();
                                return Optional.ofNullable(mycatRel.getMycatViewDataNodeMapping().apply(hintSql.getParams()).collect(Collectors.toList()));
                            } catch (Exception e) {
                                log.error("", e);
                            } finally {
                                ParamHolder.CURRENT_THREAD_LOCAL.set(null);
                            }
                        }

                    }
                }

            }
        }
        return Optional.empty();
    }

    @NotNull
    public static Function<String, BackendTableInfo> dataNodeParser() {
        return s -> {
            String[] s1 = s.split("_");
            if (s1.length >= 3) {
                return new BackendTableInfo(s1[0], s1[1], s1[2]);
            }
            if (s1.length == 2) {
                return new BackendTableInfo(null, s1[0], s1[1]);
            }
            return new BackendTableInfo(null, null, s1[0]);
        };
    }

    static class Detector extends MySqlASTVisitorAdapter {
        boolean isSharding;
        Map<String, TableHandler> tableHandlerMap = new HashMap<>();
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

        @Override
        public boolean visit(SQLExprTableSource x) {
            String tableName = x.getTableName();
            if (tableName != null) {
                TableHandler table = metadataManager.getTable(SQLUtils.normalize(x.getSchema()), SQLUtils.normalize(tableName));
                isSharding = table.getType() == LogicTableType.SHARDING;
                String alias = x.computeAlias();
                if (alias == null) {
                    alias = table.getTableName();
                } else {
                    alias = SQLUtils.normalize(alias);
                }
                tableHandlerMap.put(alias, table);
            }
            return false;
        }
    }
}

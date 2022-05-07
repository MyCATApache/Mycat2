/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.vertx;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.MycatSQLEvalVisitorUtils;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.google.common.collect.ImmutableList;
import io.mycat.*;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlObjectArrayRow;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.ExecutorProvider;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.spm.QueryPlanner;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.ServerConfig;
import io.mycat.newquery.MysqlCollector;
import io.mycat.newquery.NewMycatConnection;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import lombok.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.mycat.vertx.VertxExecuter.FillAutoIncrementType.AUTOINC_HAS_COLUMN;

public class VertxExecuter {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxExecuter.class);
    private static int rewriteinsertbatchedstatementbatch = !MetaClusterCurrent.exist(ServerConfig.class) ? 1000 : MetaClusterCurrent.wrapper(ServerConfig.class).getRewriteInsertBatchedStatementBatch();

    public static Future<long[]> runUpdate(Future<NewMycatConnection> sqlConnectionFuture, String sql, List<Object> params) {
        return sqlConnectionFuture.flatMap(c -> c.update(sql, params))
                .map(r -> new long[]{r.getAffectRows(), r.getLastInsertId()});
    }

    public static Future<long[]> simpleUpdate(MycatDataContext context, boolean insert, boolean xa, boolean onlyFirstSum, Collection<EachSQL> eachSQLs) {
        if (xa && (eachSQLs.size() > 1)) {
            return simpleUpdate(context, insert, xa, onlyFirstSum, (Iterable<EachSQL>) eachSQLs);
        }
        return simpleUpdate(context, insert, false, onlyFirstSum, (Iterable<EachSQL>) eachSQLs);
    }

    public static Future<long[]> simpleUpdate(MycatDataContext context, boolean insert, boolean xa, boolean onlyFirstSum, Iterable<EachSQL> eachSQLs) {
        Function<Void, Future<long[]>> function = new Function<Void, Future<long[]>>() {
            final long[] sum = new long[]{0, 0};

            @Override
            public Future<long[]> apply(Void unused) {
                XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
                ConcurrentHashMap<String, Future<NewMycatConnection>> map = new ConcurrentHashMap<>();
                final AtomicBoolean firstRequest = new AtomicBoolean(true);
                for (EachSQL eachSQL : eachSQLs) {

                    String target = context.resolveDatasourceTargetName(eachSQL.getTarget(), true);
                    String sql = eachSQL.getSql();
                    List<Object> params = eachSQL.getParams();

                    Future<NewMycatConnection> connectionFuture = map.computeIfAbsent(target, s -> transactionSession.getConnection(target));
                    Future<long[]> future;
                    if (insert) {
                        future = VertxExecuter.runInsert(connectionFuture, sql, params);
                    } else {
                        future = VertxExecuter.runUpdate(connectionFuture, sql, params);
                    }
                    Future<NewMycatConnection> returnConnectionFuture = future.map((Function<long[], Void>) longs2 -> {
                        if (!onlyFirstSum) {
                            synchronized (sum) {
                                sum[0] = sum[0] + longs2[0];
                                sum[1] = Math.max(sum[1], longs2[1]);
                            }
                        } else if (firstRequest.compareAndSet(true, false)) {
                            sum[0] = longs2[0];
                            sum[1] = longs2[1];
                        }
                        return null;
                    }).mapEmpty().flatMap(c -> Future.succeededFuture(connectionFuture.result()));
                    map.put(target, returnConnectionFuture);
                }
                List<Future<NewMycatConnection>> futures = new ArrayList<>(map.values());
                return CompositeFuture.join((List) futures).map(sum);
            }
        };
        if (xa) {
            return wrapAsXaTransaction(context, function);
        } else {
            return Future.succeededFuture().flatMap(o -> function.apply(null));
        }
    }

    @Getter
    @EqualsAndHashCode
    @ToString
    public static class EachSQL {
        String target;
        String sql;
        List<Object> params;

        public EachSQL(String target, String sql, List<Object> params) {
            this.target = target;
            this.sql = sql;
            this.params = params;
        }
    }

    @SneakyThrows
    public static Collection<EachSQL> explainUpdate(DrdsSqlWithParams drdsSqlWithParams, MycatDataContext context) {
        SQLUpdateStatement statement = (SQLUpdateStatement) drdsSqlWithParams.getParameterizedStatement();
        List<Object> params = drdsSqlWithParams.getParams();
        SQLExprTableSource tableSource = (SQLExprTableSource) statement.getTableSource();
        String alias = SQLUtils.normalize(tableSource.computeAlias());
        String tableName = SQLUtils.normalize(tableSource.getTableName());
        String schemaName = SQLUtils.normalize(tableSource.getSchema());

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

        TableHandler table = metadataManager.getTable(schemaName, tableName);

        switch (table.getType()) {
            case SHARDING: {
                ShardingTable shardingTable = (ShardingTable) table;
                SimpleColumnInfo primaryKey = shardingTable.getPrimaryKey();

                SQLExpr where = statement.getWhere();

                SQLSelectStatement sqlSelectStatement = new SQLSelectStatement();
                SQLSelectQueryBlock queryBlock = sqlSelectStatement.getSelect().getQueryBlock();
                queryBlock.addWhere(where);
                queryBlock.setFrom(tableSource.clone());

                Set<String> selectKeys = new HashSet<>();

                if (primaryKey != null) {
                    selectKeys.add(primaryKey.getColumnName());
                }

                ImmutableList<ShardingTable> shardingTables = (ImmutableList) ImmutableList.builder().add(shardingTable).addAll(shardingTable.getIndexTables()).build();


                Map<String, SQLExpr> columnMap = shardingTables.stream().flatMap(s -> s.getColumns().stream().filter(i -> i.isShardingKey())).map(i -> i.getColumnName())
                        .distinct().collect(Collectors.toMap(k -> k, v -> SQLUtils.toSQLExpr(v + " = ? ")));

                shardingTable.getColumns().stream().filter(i -> shardingTable.getShardingFuntion().isShardingKey(i.getColumnName())).collect(Collectors.toList());

                selectKeys.addAll(columnMap.keySet());

                for (String selectKey : selectKeys) {
                    queryBlock.addSelectItem(new SQLPropertyExpr(alias, selectKey));
                }

                DrdsSqlWithParams queryDrdsSqlWithParams = DrdsRunnerHelper.preParse(sqlSelectStatement, null);

                QueryPlanner planCache = MetaClusterCurrent.wrapper(QueryPlanner.class);
                List<CodeExecuterContext> acceptedMycatRelList = planCache.getAcceptedMycatRelList(queryDrdsSqlWithParams);
                CodeExecuterContext codeExecuterContext = acceptedMycatRelList.get(0);
                MycatView mycatRel = (MycatView) codeExecuterContext.getMycatRel();
                List<PartitionGroup> sqlMap = AsyncMycatDataContextImpl.getSqlMap(Collections.emptyMap(), mycatRel, queryDrdsSqlWithParams, drdsSqlWithParams.getHintDataNodeFilter());

                List<Partition> partitions = sqlMap.stream().map(partitionGroup -> partitionGroup.get(shardingTable.getUniqueName())).collect(Collectors.toList());
                List<EachSQL> res = new ArrayList<>(partitions.size());
                for (Partition partition : partitions) {
                    SQLUpdateStatement eachSql = statement.clone();
                    SQLExprTableSource eachTableSource = (SQLExprTableSource) eachSql.getTableSource();
                    eachTableSource.setExpr(partition.getTable());
                    eachTableSource.setSchema(partition.getSchema());
                    res.add(new EachSQL(partition.getTargetName(), eachSql.toString(), params));
                }
                if (shardingTable.getIndexTables().isEmpty()) {
                    return res;
                }

                ExecutorProvider executorProvider = MetaClusterCurrent.wrapper(ExecutorProvider.class);
                RowBaseIterator bindable = executorProvider.runAsObjectArray(context, queryDrdsSqlWithParams);
                try {
                    List<Object[]> list = new ArrayList<>();
                    while (bindable.next()) {
                        list.add(bindable.getObjects());
                    }
                    if (list.size() > 1000) {
                        throw new IllegalArgumentException("The number of update rows exceeds the limit.");
                    }

                    for (ShardingTable indexTable : shardingTable.getIndexTables()) {
                        SQLUpdateStatement eachStatement = new SQLUpdateStatement();
                        eachStatement.setFrom(new SQLExprTableSource());
                        SQLExprTableSource sqlTableSource = (SQLExprTableSource) eachStatement.getTableSource();
                        sqlTableSource.setExpr(indexTable.getTableName());
                        sqlTableSource.setSchema(indexTable.getSchemaName());

                        SQLBinaryOpExprGroup sqlBinaryOpExprGroup = new SQLBinaryOpExprGroup(SQLBinaryOperator.Equality, DbType.mysql);


                        RelDataType rowType = codeExecuterContext.getMycatRel().getRowType();

                        ArrayList<Integer> exactKeys = new ArrayList<>();
                        for (SimpleColumnInfo column : indexTable.getColumns()) {
                            SQLExpr sqlExpr = columnMap.get(column.getColumnName());
                            sqlExpr = sqlExpr.clone();
                            sqlBinaryOpExprGroup.add(sqlExpr);
                            RelDataTypeField field = rowType.getField(column.getColumnName(), false, false);
                            exactKeys.add(field.getIndex());
                        }


                        eachStatement.setWhere(sqlBinaryOpExprGroup);

                        for (Object[] eachParams : list) {
                            List<Object> newEachParams = new ArrayList<>();
                            for (Integer exactKey : exactKeys) {
                                newEachParams.add(eachParams[exactKey]);
                            }

                            Collection<EachSQL> eachSQLS = explainUpdate(new DrdsSqlWithParams(eachStatement.toString(),
                                            newEachParams,
                                            false,
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            Collections.emptyList()),
                                    context);

                            res.addAll(eachSQLS);
                        }
                    }
                    return res;
                } finally {
                    context.getTransactionSession().closeStatementState().toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS);
                }
            }
            case GLOBAL: {
                GlobalTable globalTable = (GlobalTable) table;
                List<EachSQL> res = new ArrayList<>(globalTable.getGlobalDataNode().size());
                for (Partition partition : globalTable.getGlobalDataNode()) {
                    SQLUpdateStatement eachSql = statement.clone();
                    SQLExprTableSource eachTableSource = (SQLExprTableSource) eachSql.getTableSource();
                    eachTableSource.setExpr(partition.getTable());
                    eachTableSource.setSchema(partition.getSchema());
                    res.add(new EachSQL(partition.getTargetName(), eachSql.toString(), params));
                }
                return res;
            }
            case NORMAL: {
                NormalTable normalTable = (NormalTable) table;
                List<EachSQL> res = new ArrayList<>(1);
                Partition partition = normalTable.getDataNode();
                SQLUpdateStatement eachSql = statement.clone();
                SQLExprTableSource eachTableSource = (SQLExprTableSource) eachSql.getTableSource();
                eachTableSource.setExpr(partition.getTable());
                eachTableSource.setSchema(partition.getSchema());
                res.add(new EachSQL(partition.getTargetName(), eachSql.toString(), params));
                return res;
            }
            case CUSTOM:
                throw new UnsupportedOperationException();
            default:
                throw new IllegalStateException("Unexpected value: " + table.getType());
        }
    }

    public static List<EachSQL> rewriteInsertBatchedStatements(List<EachSQL> eachSQLs) {
        if (rewriteinsertbatchedstatementbatch < 2) {
            return eachSQLs;
        }
        return rewriteInsertBatchedStatements(eachSQLs, rewriteinsertbatchedstatementbatch);
    }


    public static void main(String[] args) {
        List<EachSQL> eachSQLS = Arrays.asList(
                new EachSQL("c0", "INSERT INTO a (id) VALUES (?), (?), (?)", Arrays.asList(1, 2, 3)),
                new EachSQL("c0", "INSERT INTO a (id) VALUES (?), (?), (?)", Arrays.asList(4, 5, 6)
                ));
        List<EachSQL> eachSQLS1 = rewriteInsertBatchedStatements(eachSQLS, 3);
        System.out.println();
    }

    public static List<EachSQL> rewriteInsertBatchedStatements(Iterable<EachSQL> eachSQLs, int batchSize) {
        @Data
        @AllArgsConstructor
        @EqualsAndHashCode
        class key {
            String target;
            String sql;
        }

        Map<key, SQLInsertStatement> map = new ConcurrentHashMap<>();
        final Function<Map.Entry<key, SQLInsertStatement>, EachSQL> finalFunction = i -> {
            key key = i.getKey();
            SQLInsertStatement value = i.getValue();
            if (value.getValuesList().isEmpty()) {
                return null;
            }
            return new EachSQL(key.getTarget(), value.toString(), Collections.emptyList());
        };
        LinkedList<EachSQL> res = new LinkedList<>();
        for (EachSQL eachSQL : eachSQLs) {

            String target = eachSQL.getTarget();
            String sql = eachSQL.getSql();
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
            List<Object> sqlParams = eachSQL.getParams();

            if (sqlStatement instanceof SQLInsertStatement) {
                SQLInsertStatement insertStatement = (SQLInsertStatement) sqlStatement;


                key key = new key(target, sql);

                final SQLInsertStatement nowInsertStatement = map.computeIfAbsent(key, key1 -> {
                    SQLInsertStatement clone = (SQLInsertStatement) SQLUtils.parseSingleMysqlStatement(sql);
                    clone.getValuesList().clear();
                    return clone;
                });
                MySqlASTVisitorAdapter mySqlASTVisitorAdapter = new MySqlASTVisitorAdapter() {
                    @Override
                    public boolean visit(SQLVariantRefExpr x) {
                        SQLReplaceable parent = (SQLReplaceable) x.getParent();
                        parent.replace(x, PreparedStatement.fromJavaObject(sqlParams.get(x.getIndex())));
                        return false;
                    }
                };


                List<EachSQL> list = new LinkedList<>();
                for (SQLInsertStatement.ValuesClause valuesClause : insertStatement.getValuesList()) {
                    valuesClause = valuesClause.clone();
                    nowInsertStatement.addValueCause(valuesClause);
                    valuesClause.accept(mySqlASTVisitorAdapter);

                    if (nowInsertStatement.getValuesList().size() >= batchSize) {
                        if (nowInsertStatement.getValuesList().isEmpty()) {
                            continue;
                        }
                        EachSQL e = new EachSQL(key.getTarget(), nowInsertStatement.toString(), Collections.emptyList());
                        list.add(e);
                        nowInsertStatement.getValuesList().clear();
                    } else {
                        continue;
                    }
                }
                res.addAll(list);
            } else {
                res.add(eachSQL);
            }
        }
        res.addAll(map.entrySet().stream().map(finalFunction).filter(i -> i != null).collect(Collectors.toList()));
        return res;
    }

    @SneakyThrows
    public static List<EachSQL> explainInsert(String statementArg, List<Object> paramArg) {
        final MySqlInsertStatement statement = (MySqlInsertStatement) SQLUtils.parseSingleMysqlStatement(statementArg);

        MySqlInsertStatement templateTemp = (MySqlInsertStatement) SQLUtils.parseSingleMysqlStatement(statementArg);
        templateTemp.getColumns().clear();
        templateTemp.getValuesList().clear();
        String template = templateTemp.toString();


        SQLExprTableSource tableSource = statement.getTableSource();

        String tableName = SQLUtils.normalize(tableSource.getTableName());
        String schemaName = SQLUtils.normalize(tableSource.getSchema());

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        ShardingTable table = (ShardingTable) metadataManager.getTable(schemaName, tableName);
        SimpleColumnInfo autoIncrementColumn = table.getAutoIncrementColumn();


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        List<SQLName> columns = (List) statement.getColumns();
        if (columns.isEmpty()) {
            if (statement.getValues().getValues().size() == table.getColumns().size()) {
                for (SimpleColumnInfo column : table.getColumns()) {
                    statement.addColumn(new SQLIdentifierExpr("`" + column.getColumnName() + "`"));
                }
            }
        }
        FillAutoIncrementContext fillAutoIncrementContext = needFillAutoIncrement(table, columns);
        if (fillAutoIncrementContext.type == FillAutoIncrementType.AUTOINC_NO_COLUMN) {
            columns.add(new SQLIdentifierExpr(autoIncrementColumn.getColumnName()));
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        Map<String, Integer> columnMap = new HashMap<>();

        int index = 0;
        for (SQLName column : columns) {
            columnMap.put(SQLUtils.normalize(column.getSimpleName()), index);
            ++index;
        }

        List<List> paramsList = (!paramArg.isEmpty() && paramArg.get(0) instanceof List) ? (List) paramArg : Collections.singletonList(paramArg);

        return paramsList.stream().flatMap(params -> {
            List<EachSQL> sqls = new LinkedList<>();
            for (SQLInsertStatement.ValuesClause valuesClause : statement.getValuesList()) {

                valuesClause = valuesClause.clone();
                MySqlInsertStatement primaryStatement = (MySqlInsertStatement) SQLUtils.parseSingleMysqlStatement(template);
                primaryStatement.getColumns().addAll(columns);
                primaryStatement.getValuesList().add(valuesClause);
                List<SQLExpr> values = primaryStatement.getValues().getValues();

                switch (fillAutoIncrementContext.type) {
                    case AUTOINC_HAS_COLUMN: {
                        SQLExpr sqlExpr = values.get(fillAutoIncrementContext.existColumnIndex);
                        if (sqlExpr instanceof SQLVariantRefExpr) {
                            int paramIndex = ((SQLVariantRefExpr) sqlExpr).getIndex();
                            Object o = params.get(paramIndex);
                            if (o == null || (o instanceof Number && ((Number) o).intValue() == 0)) {
                                Supplier<Number> stringSupplier = table.nextSequence();
                                params.set(paramIndex, stringSupplier.get());
                            }
                        } else if (sqlExpr instanceof SQLNullExpr) {
                            Supplier<Number> stringSupplier = table.nextSequence();
                            primaryStatement.getValues().replace(sqlExpr, PreparedStatement.fromJavaObject(stringSupplier.get()));
                        }
                        break;
                    }
                    case AUTOINC_NO_COLUMN: {
                        Supplier<Number> stringSupplier = table.nextSequence();
                        values.add(PreparedStatement.fromJavaObject(stringSupplier.get()));
                        break;
                    }
                    case NO_AUTOINC:
                        break;
                }
                Map<String, List<RangeVariable>> variables = compute(columns, values, params);
                Partition mPartition = table.getShardingFuntion().calculateOne((Map) variables);

                SQLExprTableSource exprTableSource = primaryStatement.getTableSource();
                exprTableSource.setSimpleName(mPartition.getTable());
                exprTableSource.setSchema(mPartition.getSchema());

                if (!primaryStatement.getDuplicateKeyUpdate().isEmpty()) {
                    ArrayList<SQLExpr> exprs = new ArrayList<>(primaryStatement.getDuplicateKeyUpdate().size());
                    for (SQLExpr sqlExpr : primaryStatement.getDuplicateKeyUpdate()) {
                        SQLBinaryOpExpr op = (SQLBinaryOpExpr) sqlExpr;
                        SQLExpr right = op.getRight();
                        if (right instanceof SQLVariantRefExpr) {
                            op.setRight(io.mycat.PreparedStatement.fromJavaObject(paramArg.get(((SQLVariantRefExpr) right).getIndex())));
                        } else if (right instanceof SQLValuableExpr) {

                        } else {
                            //throw new UnsupportedOperationException("unsupported " + op);
                        }
                        exprs.add(op);
                    }
                    primaryStatement.getDuplicateKeyUpdate().clear();
                    primaryStatement.getDuplicateKeyUpdate().addAll(exprs);
                }

                sqls.add(new EachSQL(mPartition.getTargetName(), primaryStatement.toString(), getNewParams(params, primaryStatement)));


                for (ShardingTable indexTable : table.getIndexTables()) {


                    //  fillIndexTableShardingKeys(variables, indexTable);

                    Partition sPartition = indexTable.getShardingFuntion().calculateOne((Map) variables);

                    MySqlInsertStatement eachStatement = (MySqlInsertStatement) SQLUtils.parseSingleMysqlStatement(template.toString());
                    eachStatement.getColumns().clear();
                    eachStatement.getValuesList().clear();
                    ;

                    if (!eachStatement.getDuplicateKeyUpdate().isEmpty()) {
                        ArrayList<SQLExpr> exprs = new ArrayList<>(eachStatement.getDuplicateKeyUpdate().size());
                        for (SQLExpr sqlExpr : eachStatement.getDuplicateKeyUpdate()) {
                            SQLBinaryOpExpr op = (SQLBinaryOpExpr) sqlExpr;
                            String left = SQLUtils.normalize(op.getLeft().toString());
                            if (indexTable.getColumns().stream().anyMatch(i -> left.equalsIgnoreCase(i.getColumnName()))) {
                                SQLExpr right = op.getRight();
                                if (right instanceof SQLVariantRefExpr) {
                                    op.setRight(io.mycat.PreparedStatement.fromJavaObject(paramArg.get(((SQLVariantRefExpr) right).getIndex())));
                                } else if (right instanceof SQLValuableExpr) {

                                } else {
                                    // throw new UnsupportedOperationException("unsupported " + op);
                                }
                                exprs.add(op);
                            }
                        }

                        eachStatement.getDuplicateKeyUpdate().clear();
                        eachStatement.getDuplicateKeyUpdate().addAll(exprs);
                    }

                    fillIndexTableShardingKeys(columnMap, values, indexTable.getColumns(), eachStatement);

                    SQLExprTableSource eachTableSource = eachStatement.getTableSource();
                    eachTableSource.setSimpleName(sPartition.getTable());
                    eachTableSource.setSchema(sPartition.getSchema());

                    sqls.add(new EachSQL(sPartition.getTargetName(), eachStatement.toString(), getNewParams(params, eachStatement)));
                }
            }
            return sqls.stream();
        }).collect(Collectors.toList());
    }

    private static void fillIndexTableShardingKeys(Map<String, Integer> columnMap, List<SQLExpr> values, List<SimpleColumnInfo> otherColumns, SQLInsertStatement eachStatement) {
        eachStatement.getColumns().addAll(otherColumns.stream().map((Function<SimpleColumnInfo, SQLName>) i -> new SQLIdentifierExpr(i.getColumnName())).collect(Collectors.toList()));
        eachStatement.addValueCause(new SQLInsertStatement.ValuesClause(
                otherColumns.stream().map(i -> {
                    Integer integer = columnMap.get(i.getColumnName());
                    if (integer != null) {
                        return values.get(integer);
                    }
                    return new SQLNullExpr();
                }).collect(Collectors.toList())
        ));
    }

    @NotNull
    private static List<Object> getNewParams(List params, SQLInsertStatement primaryStatement) {
        List<Object> newParams = new ArrayList<>();
        primaryStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLVariantRefExpr x) {
                newParams.add(params.get(x.getIndex()));
                return false;
            }
        });
        return newParams;
    }

    static enum FillAutoIncrementType {
        AUTOINC_HAS_COLUMN,
        AUTOINC_NO_COLUMN,
        NO_AUTOINC
    }

    static class FillAutoIncrementContext {
        FillAutoIncrementType type;
        int existColumnIndex;

        public static FillAutoIncrementContext of(
                FillAutoIncrementType type,
                int existColumnIndex
        ) {
            FillAutoIncrementContext fillAutoIncrementContext = new FillAutoIncrementContext();
            fillAutoIncrementContext.existColumnIndex = existColumnIndex;
            fillAutoIncrementContext.type = type;
            return fillAutoIncrementContext;
        }
    }

    private static FillAutoIncrementContext needFillAutoIncrement(ShardingTable table, List<SQLName> columns) {
        SimpleColumnInfo autoIncrementColumn = table.getAutoIncrementColumn();
        int index = 0;
        if (autoIncrementColumn != null) {
            for (SQLName column : columns) {
                if (SQLUtils.nameEquals(column.getSimpleName(), autoIncrementColumn.getColumnName())) {
                    return FillAutoIncrementContext.of(FillAutoIncrementType.AUTOINC_HAS_COLUMN, index);
                }
                index++;
            }
            return FillAutoIncrementContext.of(FillAutoIncrementType.AUTOINC_NO_COLUMN, -1);
        } else {
            return FillAutoIncrementContext.of(FillAutoIncrementType.NO_AUTOINC, -1);
        }
    }

    public static Map<String, RangeVariable> compute(List<SQLName> columns,
                                                     List<SQLExpr> values,
                                                     List<Object> params) {
        Map<String, RangeVariable> variables = new HashMap<>(1);
        for (int i = 0; i < columns.size(); i++) {
            SQLExpr sqlExpr = values.get(i);
            Object o = null;
            if (sqlExpr instanceof SQLVariantRefExpr) {
                int index = ((SQLVariantRefExpr) sqlExpr).getIndex();
                o = params.get(index);
            } else if (sqlExpr instanceof SQLNullExpr) {
                o = null;
            } else if (sqlExpr instanceof SQLValuableExpr) {
                o = ((SQLValuableExpr) sqlExpr).getValue();
            } else {
                try {
                    o = MycatSQLEvalVisitorUtils.eval(DbType.mysql, sqlExpr, params);
                } catch (Throwable throwable) {
                    boolean success = false;
                    if (sqlExpr instanceof SQLMethodInvokeExpr) {
                        if (!((SQLMethodInvokeExpr) sqlExpr).getArguments().isEmpty()) {
                            SQLExpr sqlExpr1 = ((SQLMethodInvokeExpr) sqlExpr).getArguments().get(0);
                            if (sqlExpr1 instanceof SQLVariantRefExpr) {
                                int index = ((SQLVariantRefExpr) sqlExpr1).getIndex();
                                o = params.get(index);
                                success = true;
                            } else if (sqlExpr1 instanceof SQLCharExpr) { // 兼容 compress 等函数，放的是字符串
                                o = ((SQLCharExpr) sqlExpr1).getText();
                                success = true;
                            }
                        }
                    }
                    if (!success) {
                        LOGGER.debug("", throwable);
                    }
                }
            }
            String columnName = SQLUtils.normalize(columns.get(i).getSimpleName());
            variables.put(columnName, new RangeVariable(columnName, RangeVariableType.EQUAL, o));
        }
        return variables;
    }

    public static Future<long[]> wrapAsXaTransaction(MycatDataContext context, Function<Void, Future<long[]>> function) {
        TransactionSession sqlConnection = context.getTransactionSession();
        if ((!context.isInTransaction() && context.isAutocommit())) {
            Future<long[]> future = sqlConnection.begin().flatMap(function);
            return future.flatMap(longs -> sqlConnection.commit().map(longs))
                    .recover(throwable -> CompositeFuture.join(Future.failedFuture(throwable), sqlConnection.rollback()).mapEmpty());
        }
        return Future.succeededFuture().flatMap(o -> function.apply(null));
    }


    public static Observable<MysqlPayloadObject> runQueryOutputAsMysqlPayloadObject(Future<NewMycatConnection> connectionFuture,
                                                                                    String sql,
                                                                                    List<Object> values) {
        return Observable.create(emitter -> {
            // 连接到达
            connectionFuture.onSuccess(connection -> {
                // 预编译到达
                connection.prepareQuery(sql, values, new MysqlCollector() {

                    MycatRowMetaData mycatRowMetaData;

                    @Override
                    public void onColumnDef(MycatRowMetaData mycatRowMetaData) {
                        emitter.onNext(new MySQLColumnDef(this.mycatRowMetaData = mycatRowMetaData));
                    }

                    @Override
                    public void onRow(Object[] row) {
                        emitter.onNext(new MysqlObjectArrayRow(BaseRowObservable.getObjects(row, this.mycatRowMetaData)));
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }

                    @Override
                    public void onError(Throwable e) {
                        emitter.onError(e);
                    }
                });
            });
            connectionFuture.onFailure(i -> emitter.onError(i));
        });
    }


    public static interface Queryer<T> {
        Observable<T> runQuery(Future<NewMycatConnection> connectionFuture,
                               String sql,
                               List<Object> values,
                               MycatRowMetaData rowMetaDataArg);
    }

    public static Observable<Object[]> runQuery(Future<NewMycatConnection> connectionFuture,
                                                String sql,
                                                List<Object> values,
                                                MycatRowMetaData rowMetaDataArg) {
        return Observable.create(emitter -> {
            // 连接到达
            connectionFuture.onSuccess(connection -> {
                // 预编译到达
                connection.prepareQuery(sql, values, new MysqlCollector() {

                    MycatRowMetaData mycatRowMetaData;

                    @Override
                    public void onColumnDef(MycatRowMetaData mycatRowMetaData) {
                        this.mycatRowMetaData = Optional.ofNullable(rowMetaDataArg).orElse(mycatRowMetaData);
                    }

                    @Override
                    public void onRow(Object[] row) {
                        try {
                            emitter.onNext(BaseRowObservable.getObjects(row, this.mycatRowMetaData));
                        } catch (Exception e) {
                            LOGGER.error("", e);
                            throw e;
                        }
                    }

                    @Override
                    public void onComplete() {
                        emitter.onComplete();
                    }

                    @Override
                    public void onError(Throwable e) {
                        emitter.onError(e);
                    }
                });
            });
            connectionFuture.onFailure(i -> emitter.onError(i));
        });
    }

    public static Future<long[]> runUpdate(Future<NewMycatConnection> sqlConnectionFuture, String sql) {
        return sqlConnectionFuture.flatMap(c -> c.update(sql)
                .map(r -> new long[]{r.getAffectRows(), r.getLastInsertId()}));
    }

    public static Future<long[]> runInsert(Future<NewMycatConnection> sqlConnectionFuture, String sql) {
        return sqlConnectionFuture.flatMap(c -> c.insert(sql)
                .map(r -> new long[]{r.getAffectRows(), r.getLastInsertId()}));
    }

    public static Future<long[]> runInsert(Future<NewMycatConnection> sqlConnectionFuture, String sql, List<Object> params) {
        return sqlConnectionFuture.flatMap(c -> c.insert(sql, params)
                .map(r -> new long[]{r.getAffectRows(), r.getLastInsertId()}));
    }

    public static Future<long[]> runInsert(
            Map<String, List<List<Object>>> insertMap,
            Future<SqlConnection> sqlConnectionFuture) {
        List<long[]> list = Collections.synchronizedList(new ArrayList<>());
        Future<Void> future = Future.succeededFuture();
        for (Map.Entry<String, List<List<Object>>> e : insertMap.entrySet()) {
            String sql = e.getKey();
            List<List<Object>> values = e.getValue();
            future = future.flatMap(unused -> {
                return sqlConnectionFuture
                        .flatMap(new Function<SqlConnection, Future<Void>>() {
                            @Override
                            public Future<Void> apply(SqlConnection connection) {
                                Future<Void> future2 = connection.prepare(sql).flatMap(preparedStatement -> {
                                    List<Tuple> collect = values.stream().map(u -> Tuple.from(u)).collect(Collectors.toList());
                                    Future<RowSet<Row>> rowSetFuture = preparedStatement.query().executeBatch(collect);
                                    Future<Void> map = rowSetFuture.map(rows -> {
                                        int affectedRow = rows.rowCount();
                                        long lastInsertId = Optional.ofNullable(rows.property(MySQLClient.LAST_INSERTED_ID))
                                                .orElse(0L);
                                        list.add(new long[]{affectedRow, lastInsertId});
                                        return null;
                                    });
                                    return map;
                                });
                                return future2;
                            }
                        });
            });
        }
        return future.map(sumUpdateResult(list));
    }

    @NotNull
    private static Function<Void, long[]> sumUpdateResult(List<long[]> list) {
        return unused -> list.stream()
                .reduce(new long[]{0, 0},
                        (longs1, longs2) ->
                                new long[]{longs1[0] + longs2[0],
                                        Math.max(longs1[1], longs2[1])});
    }
}

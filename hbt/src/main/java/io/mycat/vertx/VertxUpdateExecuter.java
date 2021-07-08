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

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.mycat.*;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.MycatHint;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.spm.MemPlanCache;
import io.mycat.calcite.spm.QueryPlanCache;
import io.mycat.calcite.spm.QueryPlanner;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingTable;
import io.reactivex.rxjava3.core.Observable;
import lombok.SneakyThrows;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class VertxUpdateExecuter {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxUpdateExecuter.class);


    public static SQLExprTableSource getTableSource(SQLStatement sqlStatement) {
        if (sqlStatement instanceof SQLUpdateStatement) {
            return (SQLExprTableSource) ((SQLUpdateStatement) sqlStatement).getTableSource();
        }
        if (sqlStatement instanceof SQLDeleteStatement) {
            return (SQLExprTableSource) ((SQLDeleteStatement) sqlStatement).getTableSource();
        }
        if (sqlStatement instanceof SQLInsertStatement) {
            return (SQLExprTableSource) ((SQLInsertStatement) sqlStatement).getTableSource();
        }
        throw new UnsupportedOperationException();
    }

    public static SQLExpr getWhere(SQLStatement sqlStatement) {
        if (sqlStatement instanceof SQLUpdateStatement) {
            return ((SQLUpdateStatement) sqlStatement).getWhere();
        }
        if (sqlStatement instanceof SQLDeleteStatement) {
            return ((SQLDeleteStatement) sqlStatement).getWhere();
        }
        if (sqlStatement instanceof SQLInsertStatement) {
            return null;
        }
        throw new UnsupportedOperationException();
    }

    public static void setFrom(SQLStatement sqlStatement, SQLExprTableSource tableSource) {
        if (sqlStatement instanceof SQLUpdateStatement) {
            ((SQLUpdateStatement) sqlStatement).setFrom(tableSource);
            return;
        }
        if (sqlStatement instanceof SQLDeleteStatement) {
            ((SQLDeleteStatement) sqlStatement).setFrom(tableSource);
            return;
        }
        if (sqlStatement instanceof SQLInsertStatement) {
            ((SQLInsertStatement) sqlStatement).setTableSource(tableSource);
            return;
        }
        throw new UnsupportedOperationException();
    }
    public static void setWhere(SQLStatement sqlStatement, SQLExpr sqlExpr) {
        if (sqlStatement instanceof SQLUpdateStatement) {
            ((SQLUpdateStatement) sqlStatement).setWhere(sqlExpr);
            return;
        }
        if (sqlStatement instanceof SQLDeleteStatement) {
            ((SQLDeleteStatement) sqlStatement).setWhere(sqlExpr);
            return;
        }
        if (sqlStatement instanceof SQLInsertStatement) {
            return;
        }
        throw new UnsupportedOperationException();
    }
    @SneakyThrows
    public static Collection<VertxExecuter.EachSQL> explainUpdate(DrdsSqlWithParams drdsSqlWithParams, MycatDataContext context) {
        SQLStatement statement = drdsSqlWithParams.getParameterizedStatement();
        List<Object> maybeList = drdsSqlWithParams.getParams();
        boolean multi = !maybeList.isEmpty() && maybeList.get(0) instanceof List;
        List<List<Object>> paramList = multi?(List)maybeList:Collections.singletonList(maybeList);
        List<VertxExecuter.EachSQL> res = new ArrayList<>();
        for (List<Object> params : paramList) {
            SQLExprTableSource tableSource = (SQLExprTableSource) getTableSource(statement);
            String alias = SQLUtils.normalize(tableSource.computeAlias());
            String tableName = SQLUtils.normalize(tableSource.getTableName());
            String schemaName = SQLUtils.normalize(tableSource.getSchema());

            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

            TableHandler table = metadataManager.getTable(schemaName, tableName);

            switch (table.getType()) {
                case SHARDING: {
                    ShardingTable shardingTable = (ShardingTable) table;
                    SimpleColumnInfo primaryKey = shardingTable.getPrimaryKey();

                    SQLExpr where = getWhere(statement);

                    SQLSelectStatement sqlSelectStatement = new SQLSelectStatement();
                    SQLSelect sqlSelect = new SQLSelect();
                    sqlSelect.setQuery(new SQLSelectQueryBlock());
                    sqlSelectStatement.setSelect(sqlSelect);
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
                    List<Object> innerParams = new ArrayList<>();
                    List<SqlTypeName> innerTypes = new ArrayList<>();
                    if (where!=null) {
                        List<SqlTypeName> typeNames = drdsSqlWithParams.getTypeNames();
                        where.accept(new MySqlASTVisitorAdapter() {
                            @Override
                            public boolean visit(SQLVariantRefExpr x) {
                                innerParams.add(params.get(x.getIndex()));
                                innerTypes.add(typeNames.get(x.getIndex()));
                                return false;
                            }
                        });
                    }
                    DrdsSqlWithParams queryDrdsSqlWithParams =
                   new DrdsSqlWithParams(sqlSelectStatement.toString(),
                           innerParams,
                    false,
                           innerTypes,
                   Collections.emptyList(),
                   Collections.emptyList());
                    QueryPlanner planCache = MetaClusterCurrent.wrapper(QueryPlanner.class);
                    List<CodeExecuterContext> acceptedMycatRelList = planCache.getAcceptedMycatRelList(queryDrdsSqlWithParams);
                    CodeExecuterContext codeExecuterContext = acceptedMycatRelList.get(0);
                    MycatView mycatRel = (MycatView) codeExecuterContext.getMycatRel();
                    List<PartitionGroup> sqlMap = AsyncMycatDataContextImpl.getSqlMap(Collections.emptyMap(), mycatRel, queryDrdsSqlWithParams, drdsSqlWithParams.getHintDataNodeFilter());

                    List<Partition> partitions = sqlMap.stream().map(partitionGroup -> partitionGroup.get(shardingTable.getUniqueName())).collect(Collectors.toList());
                    for (Partition partition : partitions) {
                        SQLStatement eachSql = statement.clone();
                        SQLExprTableSource eachTableSource = getTableSource(eachSql);
                        eachTableSource.setExpr(partition.getTable());
                        eachTableSource.setSchema(partition.getSchema());
                        res.add(new VertxExecuter.EachSQL(partition.getTargetName(), eachSql.toString(), params));
                    }
                    if (shardingTable.getIndexTables().isEmpty()) {
                      continue;
                    }
                    AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext =
                            new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(context, codeExecuterContext, queryDrdsSqlWithParams);

                    ArrayBindable bindable = codeExecuterContext.getBindable();

                    Object bindObservable = bindable.bindObservable(sqlMycatDataContext);
                    try {
                        List<Object[]> objects;
                        if (bindObservable instanceof Observable) {
                            objects = ((Observable<Object[]>) bindObservable).toList().blockingGet();
                        } else {
                            objects = ((Enumerable<Object[]>) (Enumerable) bindObservable).toList();
                        }

                        Object[][] list = Iterables.toArray(objects, Object[].class);
                        if (list.length > 1000) {
                            throw new IllegalArgumentException("The number of update rows exceeds the limit.");
                        }

                        for (ShardingTable indexTable : shardingTable.getIndexTables()) {
                            SQLStatement eachStatement = new SQLUpdateStatement();
                            setFrom(eachStatement, new SQLExprTableSource());
                            SQLExprTableSource sqlTableSource = getTableSource(eachStatement);
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


                            setWhere(eachStatement,sqlBinaryOpExprGroup);

                            for (Object[] eachParams : list) {
                                List<Object> newEachParams = new ArrayList<>();
                                for (Integer exactKey : exactKeys) {
                                    newEachParams.add(eachParams[exactKey]);
                                }

                                Collection<VertxExecuter.EachSQL> eachSQLS = VertxUpdateExecuter.explainUpdate(new DrdsSqlWithParams(eachStatement.toString(),
                                                newEachParams,
                                                false,
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                Collections.emptyList()),
                                        context);

                                res.addAll(eachSQLS);
                            }
                        }
                    continue;
                    } finally {
                        context.getTransactionSession().closeStatementState().toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS);
                    }
                }
                case GLOBAL: {
                    GlobalTable globalTable = (GlobalTable) table;

                    for (Partition partition : globalTable.getGlobalDataNode()) {
                        SQLStatement eachSql = statement.clone();
                        SQLExprTableSource eachTableSource = getTableSource(eachSql);
                        eachTableSource.setExpr(partition.getTable());
                        eachTableSource.setSchema(partition.getSchema());
                        res.add(new VertxExecuter.EachSQL(partition.getTargetName(), eachSql.toString(), params));
                    }
            continue;
                }
                case NORMAL: {
                    NormalTable normalTable = (NormalTable) table;
                    Partition partition = normalTable.getDataNode();
                    SQLStatement eachSql = statement.clone();
                    SQLExprTableSource eachTableSource = getTableSource(eachSql);
                    eachTableSource.setExpr(partition.getTable());
                    eachTableSource.setSchema(partition.getSchema());
                    res.add(new VertxExecuter.EachSQL(partition.getTargetName(), eachSql.toString(), params));
       continue;
                }
                case CUSTOM:
                    throw new UnsupportedOperationException();
                default:
                    throw new IllegalStateException("Unexpected value: " + table.getType());
            }
        }
        return res;

    }

}

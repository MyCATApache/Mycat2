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

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.ExecutorProvider;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.spm.QueryPlanner;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingIndexTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.newquery.RowSet;
import io.reactivex.rxjava3.core.Observable;
import lombok.SneakyThrows;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.mycat.calcite.DrdsRunnerHelper.getTypes;

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
        if (sqlStatement instanceof SQLReplaceStatement) {
            return (SQLExprTableSource) ((SQLReplaceStatement) sqlStatement).getTableSource();
        }
        throw new UnsupportedOperationException(sqlStatement+" can not get SQLExprTableSource");
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
        if (sqlStatement instanceof SQLReplaceStatement) {
            return null;
        }
       return null;
    }

    public static void setFrom(SQLStatement sqlStatement, SQLExprTableSource tableSource) {
        if (sqlStatement instanceof SQLUpdateStatement) {
            SQLUpdateStatement sqlStatement1 = (SQLUpdateStatement) sqlStatement;
            // sqlStatement1.setFrom(tableSource);
            sqlStatement1.setTableSource(tableSource);
            return;
        }
        if (sqlStatement instanceof SQLDeleteStatement) {
            SQLDeleteStatement sqlStatement1 = (SQLDeleteStatement) sqlStatement;
            // sqlStatement1.setFrom(tableSource);
            sqlStatement1.setTableSource(tableSource);
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
        List<List<Object>> paramList = multi ? (List) maybeList : Collections.singletonList(maybeList);
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

                    if (where == null) {
                        handleEmptyWhereSharding(statement, res, params, shardingTable);
                        continue;
                    }

                    SQLSelectStatement sqlSelectStatement = new SQLSelectStatement();
                    SQLSelect sqlSelect = new SQLSelect();
                    sqlSelect.setQuery(new SQLSelectQueryBlock());
                    sqlSelectStatement.setSelect(sqlSelect);
                    SQLSelectQueryBlock queryBlock = sqlSelectStatement.getSelect().getQueryBlock();
                    queryBlock.addWhere(where);
                    queryBlock.setFrom(tableSource.clone());

                    Set<String> selectKeys = getSelectKeys(shardingTable, primaryKey);

                    for (String selectKey : selectKeys) {
                        queryBlock.addSelectItem(new SQLPropertyExpr(alias, selectKey), selectKey);
                    }
                    List<Object> innerParams = new ArrayList<>();
                    List<SqlTypeName> innerTypes = new ArrayList<>();
                    List<SqlTypeName> typeNames = drdsSqlWithParams.getTypeNames();
                    where.accept(new MySqlASTVisitorAdapter() {
                        @Override
                        public boolean visit(SQLVariantRefExpr x) {
                            innerParams.add(params.get(x.getIndex()));
                            if (!typeNames.isEmpty()) {
                                innerTypes.add(typeNames.get(x.getIndex()));
                            }
                            return false;
                        }
                    });
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
                    handleTargets(statement, res, params, partitions);
                    if (shardingTable.getIndexTables().isEmpty()) {
                        continue;
                    }
                    ////////////////////////////////////////index-scan////////////////////////////////////////////////////////////
                    Objects.requireNonNull(shardingTable.getPrimaryKey()," need primary key");

                    RowBaseIterator bindable = MetaClusterCurrent.wrapper(ExecutorProvider.class).runAsObjectArray(context,sqlSelectStatement.toString());

                    try {
                        List<Object[]> list = new ArrayList<>();
                        while (bindable.next()){
                            list.add( bindable.getObjects());
                        }
                        if (list.size() > 1000) {
                            throw new IllegalArgumentException("The number of update rows exceeds the limit.");
                        }

                        for (ShardingTable indexTable : shardingTable.getIndexTables()) {
                            SQLStatement eachStatement = SQLUtils.parseSingleMysqlStatement(drdsSqlWithParams.getParameterizedSql());
                            SQLExprTableSource sqlTableSource = new SQLExprTableSource();
                            sqlTableSource.setExpr(indexTable.getTableName());
                            sqlTableSource.setSchema(indexTable.getSchemaName());

                            setFrom(eachStatement, sqlTableSource);

                            RelDataType rowType = codeExecuterContext.getMycatRel().getRowType();
                            List<SQLExpr> backConditions = getBackCondition(selectKeys, indexTable, rowType);
                            setWhere(eachStatement, backConditions.stream().reduce((sqlExpr, sqlExpr2) -> SQLBinaryOpExpr.and(sqlExpr, sqlExpr2))
                                    .orElse(null));

                            for (Object[] eachParams : list) {
                                List<Object> newEachParams = getNewEachParams(backConditions, eachParams);
                                Collection<VertxExecuter.EachSQL> eachSQLS = VertxUpdateExecuter
                                        .explainUpdate(new DrdsSqlWithParams(eachStatement.toString(),
                                                        newEachParams,
                                                        false,
                                                        getTypes(newEachParams),
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
                    handleTargets(statement, res, params, globalTable.getGlobalDataNode());
                    continue;
                }
                case NORMAL: {
                    handleNormal(statement, res, params, (NormalTable) table);
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

    @NotNull
    private static List<Object> getNewEachParams(List<SQLExpr> backConditions, Object[] eachParams) {
        List<Object> newEachParams = new ArrayList<>();
        for (SQLExpr backCondition : backConditions) {
            SQLBinaryOpExpr condition = (SQLBinaryOpExpr) backCondition;
            SQLVariantRefExpr conditionRight = (SQLVariantRefExpr) condition.getRight();
            newEachParams.add(eachParams[conditionRight.getIndex()]);
        }
        return newEachParams;
    }

    @NotNull
    private static List<SQLExpr> getBackCondition(Set<String> selectKeys, ShardingTable indexTable, RelDataType rowType) {
        List<SQLExpr> backConditions;
        List<SQLExpr> tmp = new ArrayList<>();
        for (SimpleColumnInfo column : indexTable.getColumns()) {
            if (selectKeys.contains(column.getColumnName())) {
                RelDataTypeField field = rowType.getField(column.getColumnName(), false, false);
                int index = field.getIndex();
                SQLVariantRefExpr quesVarRefExpr = new SQLVariantRefExpr("?");
                quesVarRefExpr.setIndex(index);
                tmp.add(SQLBinaryOpExpr.eq(new SQLIdentifierExpr(column.getColumnName()), quesVarRefExpr));
            }
        }
        backConditions = tmp;
        return backConditions;
    }

    private static Set<String> getSelectKeys(ShardingTable shardingTable, SimpleColumnInfo primaryKey) {
        Set<String> selectKeys;
        ImmutableList<ShardingTable> shardingTables = (ImmutableList) ImmutableList.builder().add(shardingTable).addAll(shardingTable.getIndexTables()).build();
        selectKeys = shardingTables.stream().flatMap(s -> {
            return s.getColumns().stream().filter(i -> i.isShardingKey());
        }).map(i -> i.getColumnName()).collect(Collectors.toSet());

        if (primaryKey != null) {
            selectKeys.add(primaryKey.getColumnName());
        }
        return selectKeys;
    }

    private static void handleEmptyWhereSharding(SQLStatement statement, List<VertxExecuter.EachSQL> res, List<Object> params, ShardingTable shardingTable) {
        handleTargets(statement.clone(), res, params, shardingTable.getBackends());
        for (ShardingIndexTable indexTable : shardingTable.getIndexTables()) {
            handleTargets(statement.clone(), res, params, indexTable.getBackends());
        }
    }

    private static void handleTargets(SQLStatement statement, List<VertxExecuter.EachSQL> res, List<Object> params, List<Partition> partitions) {
        for (Partition partition : partitions) {
            SQLStatement eachSql = statement.clone();
            SQLExprTableSource eachTableSource = getTableSource(eachSql);
            eachTableSource.setExpr(partition.getTable());
            eachTableSource.setSchema(partition.getSchema());
            res.add(new VertxExecuter.EachSQL(partition.getTargetName(), eachSql.toString(), params));
        }
    }

    private static void handleNormal(SQLStatement statement, List<VertxExecuter.EachSQL> res, List<Object> params, NormalTable table) {
        NormalTable normalTable = table;
        Partition partition = normalTable.getDataNode();
        SQLStatement eachSql = statement.clone();
        SQLExprTableSource eachTableSource = getTableSource(eachSql);
        eachTableSource.setExpr(partition.getTable());
        eachTableSource.setSchema(partition.getSchema());
        res.add(new VertxExecuter.EachSQL(partition.getTargetName(), eachSql.toString(), params));
    }

}

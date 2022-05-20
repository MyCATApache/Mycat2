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
package io.mycat.calcite.plan;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import io.mycat.*;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.calcite.ExecutorProvider;
import io.mycat.calcite.PrepareExecutor;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.mycat.util.Pair;
import io.mycat.vertx.VertxExecuter;
import io.mycat.vertx.VertxUpdateExecuter;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import org.apache.calcite.runtime.ArrayBindable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.mycat.vertx.VertxExecuter.FillAutoIncrementType.AUTOINC_HAS_COLUMN;
import static io.mycat.vertx.VertxExecuter.FillAutoIncrementType.AUTOINC_NO_COLUMN;


public class ObservablePlanImplementorImpl implements PlanImplementor {
    protected final static Logger LOGGER = LoggerFactory.getLogger(ObservablePlanImplementorImpl.class);
    protected final XaSqlConnection xaSqlConnection;
    protected final MycatDataContext context;
    protected final DrdsSqlWithParams drdsSqlWithParams;
    protected final Response response;

    public ObservablePlanImplementorImpl(XaSqlConnection xaSqlConnection, MycatDataContext context, DrdsSqlWithParams drdsSqlWithParams, Response response) {
        this.xaSqlConnection = xaSqlConnection;
        this.context = context;
        this.drdsSqlWithParams = drdsSqlWithParams;
        this.response = response;
    }

    @Override
    public Future<Void> executeUpdate(Plan plan) {
        MycatUpdateRel mycatRel = (MycatUpdateRel) plan.getMycatRel();
        Collection<VertxExecuter.EachSQL> eachSQLS = VertxUpdateExecuter.explainUpdate(drdsSqlWithParams, context);
        Future<long[]> future = VertxExecuter.simpleUpdate(context, mycatRel.isInsert(), true, mycatRel.isGlobal(), eachSQLS);
        return future.eventually(u -> context.getTransactionSession().closeStatementState())
                .flatMap(result -> response.sendOk(result[0], result[1]));
    }

    @Override
    public Future<Void> executeInsert(Plan logical) {
        Future<long[]> future;
        MycatInsertRel mycatRel = (MycatInsertRel) logical.getMycatRel();
        List<VertxExecuter.EachSQL> insertSqls;
        if (mycatRel.isGlobal()) {
            switch (mycatRel.sequenceType) {
                case NO_SEQUENCE:
                case GLOBAL_SEQUENCE:
                    insertSqls = VertxExecuter.explainInsert(drdsSqlWithParams.getParameterizedSQL(), drdsSqlWithParams.getParams());
                    future = VertxExecuter.simpleUpdate(context, true, true, true, insertSqls);
                    break;
                case FIRST_SEQUENCE:
                    future = executeGlobalInsertFirstSequence(drdsSqlWithParams.getParameterizedSQL(), drdsSqlWithParams.getParams());
                    break;
                default:
                    throw new UnsupportedOperationException("" + mycatRel.sequenceType);
            }
        } else {
            insertSqls = VertxExecuter.explainInsert(drdsSqlWithParams.getParameterizedSQL(), drdsSqlWithParams.getParams());
            assert !insertSqls.isEmpty();
            if (insertSqls.size() > 1) {
                future = VertxExecuter.simpleUpdate(context, true, true, mycatRel.isGlobal(), VertxExecuter.rewriteInsertBatchedStatements(insertSqls));
            } else {
                future = VertxExecuter.simpleUpdate(context, true, false, mycatRel.isGlobal(), insertSqls);
            }
        }
        return future.eventually(u -> context.getTransactionSession().closeStatementState())
                .flatMap(result -> response.sendOk(result[0], result[1]));
    }

    private Future<long[]> executeGlobalInsertFirstSequence(String parameterizedSQL, List<Object> params) {
        final MySqlInsertStatement statement = (MySqlInsertStatement) SQLUtils.parseSingleMysqlStatement(parameterizedSQL);
        SQLExprTableSource tableSource = statement.getTableSource();
        String tableName = SQLUtils.normalize(tableSource.getTableName());
        String schemaName = SQLUtils.normalize(tableSource.getSchema());
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        GlobalTable table = (GlobalTable) metadataManager.getTable(schemaName, tableName);
        List<Partition> globalDataNode = table.getGlobalDataNode();
        if (globalDataNode.isEmpty()) {
            return Future.succeededFuture(new long[]{0, 0});
        }
        if (globalDataNode.size() == 1) {
            Partition partition = globalDataNode.get(0);
            MycatSQLExprTableSourceUtil.setSqlExprTableSource(partition.getSchema(), partition.getTable(), tableSource);
            return VertxExecuter.simpleUpdate(context, true, false, false,
                    Collections.singletonList(new VertxExecuter.EachSQL(partition.getTargetName(), statement.toString(), params)));
        } else {
            return VertxExecuter.wrapAsXaTransaction(context, unused -> {
                List<SQLExpr> columns = statement.getColumns();
                if (columns.isEmpty()) {
                    columns = table.getColumns().stream()
                            .map(i -> new SQLIdentifierExpr("`" + i.getColumnName() + "`")).collect(Collectors.toList());
                    for (SQLExpr column : columns) {
                        statement.addColumn(column);
                    }
                }
                VertxExecuter.FillAutoIncrementContext fillAutoIncrementContext = VertxExecuter.needFillAutoIncrement(table, (List) statement.getColumns());
                switch (fillAutoIncrementContext.getType()) {
                    case AUTOINC_HAS_COLUMN:
                    case AUTOINC_NO_COLUMN: {
                        Set<SQLInsertStatement> needMySQLBackwardColumnSet = Collections.newSetFromMap(new IdentityHashMap<>());
                        String restore = ParameterizedOutputVisitorUtils.restore(parameterizedSQL, DbType.mysql, params);
                        List<SQLInsertStatement> sqlInsertStatements = SQLUtils.splitInsertValues(DbType.mysql, restore, 1);

                        for (SQLInsertStatement sqlInsertStatement : sqlInsertStatements) {
                            if (fillAutoIncrementContext.getType() == AUTOINC_NO_COLUMN) {
                                needMySQLBackwardColumnSet.add(sqlInsertStatement);
                                sqlInsertStatement.addColumn(new SQLIdentifierExpr("`" + table.getAutoIncrementColumn().getColumnName() + "`"));
                                sqlInsertStatement.getValues().addValue(new SQLNullExpr());
                                continue;
                            }
                            if (fillAutoIncrementContext.getType() == AUTOINC_HAS_COLUMN) {
                                List<SQLExpr> values = sqlInsertStatement.getValues().getValues();
                                SQLExpr sqlExpr = values.get(fillAutoIncrementContext.existColumnIndex);
                                if (sqlExpr instanceof SQLNullExpr) {
                                    needMySQLBackwardColumnSet.add(sqlInsertStatement);
                                } else if (sqlExpr instanceof SQLNumericLiteralExpr) {
                                    SQLNumericLiteralExpr sqlNumericLiteralExpr = (SQLNumericLiteralExpr) sqlExpr;
                                    Number number = sqlNumericLiteralExpr.getNumber();
                                    if (number == null || number.equals(0)) {//may be Double
                                        needMySQLBackwardColumnSet.add(sqlInsertStatement);
                                    }
                                }
                                continue;
                            }
                        }
                        Partition masterPartition = globalDataNode.get(0);
                        Future<Void> sequenceFuture = Future.succeededFuture();
                        List<Pair<SQLInsertStatement, long[]>> insertMap = (List) Collections.synchronizedList(new ArrayList<>());
                        for (SQLInsertStatement sqlInsertStatement : sqlInsertStatements) {
                            sequenceFuture = sequenceFuture
                                    .flatMap(aLong -> VertxExecuter.simpleUpdate(context, true, false, false,
                                                    Collections.singletonList(new VertxExecuter.EachSQL(masterPartition.getTargetName(), sqlInsertStatement.toString(), Collections.emptyList())))
                                            .map(longs -> {
                                                insertMap.add(Pair.of(sqlInsertStatement, longs));
                                                return null;
                                            }));
                        }
                        return sequenceFuture.flatMap(b -> {
                            ArrayList<Pair<SQLInsertStatement, long[]>> entries = new ArrayList<>(insertMap);
                            for (Pair<SQLInsertStatement, long[]> entry : entries) {
                                if (!needMySQLBackwardColumnSet.contains(entry.getKey())) {
                                    continue;
                                }
                                SQLInsertStatement.ValuesClause values = entry.getKey().getValues();
                                SQLExpr sqlExpr;
                                if (fillAutoIncrementContext.getType() == AUTOINC_NO_COLUMN) {
                                    int size = values.getValues().size();
                                    sqlExpr = values.getValues().get(size - 1);
                                } else if (fillAutoIncrementContext.getType() == AUTOINC_HAS_COLUMN) {

                                    sqlExpr = values.getValues().get(fillAutoIncrementContext.existColumnIndex);
                                } else {
                                    return Future.failedFuture("global insert ValuesClause is no match");
                                }
                                values.replace(sqlExpr, SQLExprUtils.fromJavaObject(entry.getValue()[1]));
                            }
                            MySqlInsertStatement template = (MySqlInsertStatement) SQLUtils.parseSingleMysqlStatement(parameterizedSQL);
                            if (fillAutoIncrementContext.getType() == AUTOINC_NO_COLUMN) {
                                template.addColumn(new SQLIdentifierExpr("`" + table.getAutoIncrementColumn().getColumnName() + "`"));
                            }
                            template.getValuesList().clear();
                            long affectRow = 0;
                            long lastId = 0;
                            for (Pair<SQLInsertStatement, long[]> entry : entries) {
                                template.addValueCause(entry.getKey().getValues());
                                long[] longs = entry.getValue();
                                affectRow = longs[0] + affectRow;
                                lastId = Math.max(lastId, longs[1]);
                            }
                            String sql = template.toString();
                            ArrayList<VertxExecuter.EachSQL> resList = new ArrayList<>();
                            for (Partition partition : globalDataNode.subList(1, globalDataNode.size())) {
                                resList.add(new VertxExecuter.EachSQL(partition.getTargetName(), sql, Collections.emptyList()));
                            }
                            long[] res = {affectRow, lastId};
                            return VertxExecuter.simpleUpdate(context, true, false, false,
                                    resList).map(longs -> res);
                        });
                    }
                    case NO_AUTOINC: {
                        ArrayList<VertxExecuter.EachSQL> eachSQLS = new ArrayList<>();
                        for (Partition partition : globalDataNode) {
                            MycatSQLExprTableSourceUtil.setSqlExprTableSource(partition.getSchema(), partition.getTable(), tableSource);
                            eachSQLS.add(new VertxExecuter.EachSQL(partition.getTargetName(), statement.toString(), params));
                        }
                        return VertxExecuter.simpleUpdate(context, true, true, true, eachSQLS);
                    }
                    default:
                        throw new IllegalStateException("Unexpected value: " + fillAutoIncrementContext.getType());
                }
            });
        }
    }

    @Override
    public Future<Void> executeQuery(Plan plan) {
        AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext = new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(context, plan.getCodeExecuterContext(), drdsSqlWithParams);
        ExecutorProvider executorProvider = MetaClusterCurrent.wrapper(ExecutorProvider.class);

        PrepareExecutor prepare = executorProvider.prepare(plan);
        if (context.isVector()) {
            PrepareExecutor.ArrowObservable observable1 = prepare.asObservableVector(sqlMycatDataContext, plan.getMetaData());
            return response.sendVectorResultSet(observable1.getMycatRowMetaData(), observable1.getObservable());
        } else {
            ArrayBindable arrayBindable = prepare.getArrayBindable();
            Observable<MysqlPayloadObject> observable1 = PrepareExecutor
                    .getMysqlPayloadObjectObservable(arrayBindable, sqlMycatDataContext, plan.getMetaData());
            Observable observable = mapToTimeoutObservable(observable1, drdsSqlWithParams);
            String parameterizedSQL = drdsSqlWithParams.getParameterizedSQL();
            //Future<Void> take = TimeRateLimiterService.STRING_INSTANCE.take(parameterizedSQL);
            Observable<MysqlPayloadObject> executor = observable;
            return response.sendResultSet(executor);
        }
    }

    public <T> Observable<T> mapToTimeoutObservable(Observable<T> observable, DrdsSqlWithParams drdsSqlWithParams) {
        Optional<Long> timeout = drdsSqlWithParams.getTimeout();
        if (timeout.isPresent()) {
            return observable.timeout(timeout.get(), TimeUnit.MILLISECONDS);
        }
        return observable;
    }


}

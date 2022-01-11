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
package io.mycat.sqlhandler.dml;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.*;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.calcite.plan.PlanImplementor;
import io.mycat.calcite.rewriter.OptimizationContext;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.UpdatePlanCache;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.prototypeserver.mysql.HackRouter;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.NameMap;
import io.mycat.util.Pair;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

public class UpdateSQLHandler extends AbstractSQLHandler<MySqlUpdateStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlUpdateStatement> request, MycatDataContext dataContext, Response response) {
        return updateHandler(request.getAst(), dataContext, request.getAst().getTableSource(), response);
    }

    @SneakyThrows
    public static Future<Void> updateHandler(SQLStatement sqlStatement, MycatDataContext dataContext, SQLTableSource tableSourceArg, Response receiver) {
        boolean insert = sqlStatement instanceof SQLInsertStatement;
        if (tableSourceArg instanceof SQLExprTableSource) {
            SQLExprTableSource tableSource = (SQLExprTableSource) tableSourceArg;
            String schemaName = Optional.ofNullable(tableSource.getSchema() == null ? dataContext.getDefaultSchema() : tableSource.getSchema())
                    .map(i -> SQLUtils.normalize(i)).orElse(null);
            String tableName = SQLUtils.normalize(tableSource.getTableName());
            if (schemaName == null) {
                return receiver.sendError("unknown schema", MySQLErrorCode.ER_UNKNOWN_ERROR);
            }
            tableSource.setSchema(schemaName);
            SchemaHandler schemaHandler;
            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            Optional<NameMap<SchemaHandler>> handlerMapOptional = Optional.ofNullable(metadataManager.getSchemaMap());
            Optional<String> targetNameOptional = Optional.ofNullable(metadataManager.getPrototype());
            if (!handlerMapOptional.isPresent()) {
                if (targetNameOptional.isPresent()) {
                    if (insert) {
                        DrdsSqlWithParams drdsSqlWithParams = MycatPreparedStatementUtil.outputToParameterizedProxySql((MySqlInsertStatement) sqlStatement);
                        return receiver.proxyInsert(Collections.singletonList(targetNameOptional.get()),
                                drdsSqlWithParams.getParameterizedSQL(),
                                drdsSqlWithParams.getParams());
                    } else {
                        DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sqlStatement, dataContext.getDefaultSchema());
                        return receiver.proxyUpdate(Collections.singletonList(targetNameOptional.get()),
                                drdsSqlWithParams.getParameterizedSQL(),
                                drdsSqlWithParams.getParams());
                    }
                } else {
                    return receiver.sendError(new MycatException("Unable to route:" + sqlStatement));
                }
            } else {
                NameMap<SchemaHandler> handlerMap = handlerMapOptional.get();
                schemaHandler = Optional.ofNullable(handlerMap.get(schemaName))
                        .orElseGet(() -> {
                            if (dataContext.getDefaultSchema() == null) {
                                throw new MycatException("unknown schema:" + schemaName);//可能schemaName有值,但是值名不是配置的名字
                            }
                            return handlerMap.get(dataContext.getDefaultSchema());
                        });
                if (schemaHandler == null) {
                    return receiver.sendError(new MycatException("Unable to route:" + sqlStatement));
                }
            }
            String defaultTargetName = schemaHandler.defaultTargetName();
            NameMap<TableHandler> tableMap = schemaHandler.logicTables();
            TableHandler tableHandler = tableMap.get(tableName);
            ///////////////////////////////common///////////////////////////////
            if (tableHandler == null) {
                if (insert) {
                    DrdsSqlWithParams drdsSqlWithParams = MycatPreparedStatementUtil.outputToParameterizedProxySql((MySqlInsertStatement) sqlStatement);
                    return receiver.proxyInsert(
                            Collections.singletonList(Objects.requireNonNull(defaultTargetName, "can not route :" + drdsSqlWithParams)),
                            drdsSqlWithParams.getParameterizedSQL(), drdsSqlWithParams.getParams());
                } else {
                    DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sqlStatement, null);

                    return receiver.proxyUpdate(
                            Collections.singletonList(Objects.requireNonNull(defaultTargetName, "can not route :" + drdsSqlWithParams)),
                            drdsSqlWithParams.getParameterizedSQL(), drdsSqlWithParams.getParams());
                }
            }
            switch (tableHandler.getType()) {
                case NORMAL:
                    DrdsSqlWithParams drdsSqlWithParams;
                    if (insert) {
                        drdsSqlWithParams = MycatPreparedStatementUtil.outputToParameterizedProxySql((MySqlInsertStatement) sqlStatement);
                    } else {
                        drdsSqlWithParams = DrdsRunnerHelper.preParse(sqlStatement, dataContext.getDefaultSchema());
                    }
                    HackRouter hackRouter = new HackRouter(drdsSqlWithParams.getParameterizedStatement(), dataContext);
                    if (hackRouter.analyse()) {
                        Pair<String, String> plan = hackRouter.getPlan();
                        if (insert) {
                            return receiver.proxyInsert(Collections.singletonList(plan.getKey()), plan.getValue(), drdsSqlWithParams.getParams());
                        } else {
                            return receiver.proxyUpdate(Collections.singletonList(plan.getKey()), plan.getValue(), drdsSqlWithParams.getParams());
                        }
                    }
                    break;
                default:
                    break;
            }
            DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sqlStatement, dataContext.getDefaultSchema());

            return executeUpdate(drdsSqlWithParams, dataContext, receiver, schemaName);
        }
        HackRouter hackRouter = new HackRouter(sqlStatement, dataContext);
        if (hackRouter.analyse()) {
            Pair<String, String> plan = hackRouter.getPlan();
            if (insert) {
                DrdsSqlWithParams drdsSqlWithParams = MycatPreparedStatementUtil.outputToParameterizedProxySql((MySqlInsertStatement) sqlStatement);
                return receiver.proxyInsert(Collections.singletonList(plan.getKey()), plan.getValue(), drdsSqlWithParams.getParams());
            } else {
                DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sqlStatement, dataContext.getDefaultSchema());

                return receiver.proxyUpdate(Collections.singletonList(plan.getKey()), plan.getValue(), drdsSqlWithParams.getParams());
            }
        }
        return receiver.sendError(new MycatException("can not route " + sqlStatement));
    }

    public static Future<Void> executeUpdate(DrdsSqlWithParams drdsSqlWithParams, MycatDataContext dataContext, Response receiver, String schemaName) {
        Plan plan = getPlan(drdsSqlWithParams);
        PlanImplementor planImplementor = DrdsRunnerHelper.getPlanImplementor(dataContext, receiver, drdsSqlWithParams);
        return DrdsRunnerHelper.impl(plan, planImplementor);
    }

    @Nullable
    public static Plan getPlan(DrdsSqlWithParams drdsSqlWithParams) {
        UpdatePlanCache updatePlanCache = MetaClusterCurrent.wrapper(UpdatePlanCache.class);
        Plan plan = updatePlanCache.computeIfAbsent(drdsSqlWithParams.getParameterizedSQL(), s -> {
            DrdsSqlCompiler drdsRunner = MetaClusterCurrent.wrapper(DrdsSqlCompiler.class);
            OptimizationContext optimizationContext = new OptimizationContext();
            MycatRel dispatch = drdsRunner.dispatch(optimizationContext, drdsSqlWithParams);
            return DrdsExecutorCompiler.convertToExecuter(
                    dispatch);
        });
        return plan;
    }
}

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
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.*;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.HackRouter;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.NameMap;
import io.mycat.util.Pair;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import lombok.SneakyThrows;

import java.util.Objects;
import java.util.Optional;

public class UpdateSQLHandler extends AbstractSQLHandler<MySqlUpdateStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlUpdateStatement> request, MycatDataContext dataContext, Response response) {
        return updateHandler(request.getAst(), dataContext, (SQLExprTableSource) request.getAst().getTableSource(), response);
    }

    @SneakyThrows
    public static Future<Void> updateHandler(SQLStatement sqlStatement, MycatDataContext dataContext, SQLExprTableSource tableSource, Response receiver) {
        String schemaName = Optional.ofNullable(tableSource.getSchema() == null ? dataContext.getDefaultSchema() : tableSource.getSchema())
                .map(i -> SQLUtils.normalize(i)).orElse(null);
        String tableName = SQLUtils.normalize(tableSource.getTableName());
        SchemaHandler schemaHandler;
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        Optional<NameMap<SchemaHandler>> handlerMapOptional = Optional.ofNullable(metadataManager.getSchemaMap());
        Optional<String> targetNameOptional = Optional.ofNullable(metadataManager.getPrototype());
        if (!handlerMapOptional.isPresent()) {
            if (targetNameOptional.isPresent()) {
                return receiver.proxyUpdate(targetNameOptional.get(), Objects.toString(sqlStatement));
            } else {
                return receiver.sendError(new MycatException("Unable to route:" + sqlStatement));
            }
        } else {
            NameMap< SchemaHandler> handlerMap = handlerMapOptional.get();
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
        NameMap< TableHandler> tableMap = schemaHandler.logicTables();
        TableHandler tableHandler = tableMap.get(tableName);
        ///////////////////////////////common///////////////////////////////
        if (tableHandler == null) {
            return receiver.proxyUpdate(
                    Objects.requireNonNull(defaultTargetName,"can not route :"+sqlStatement),
                    sqlStatement.toString());
        }
        switch (tableHandler.getType()) {
            case NORMAL:
                HackRouter hackRouter = new HackRouter(sqlStatement, dataContext);
                if(hackRouter.analyse()){
                    Pair<String, String> plan = hackRouter.getPlan();
                    return receiver.proxyUpdate(plan.getKey(),plan.getValue());
                }
                break;
            default:
                break;
        }
        DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
        return drdsRunner.runOnDrds(dataContext,sqlStatement,receiver);
    }
}

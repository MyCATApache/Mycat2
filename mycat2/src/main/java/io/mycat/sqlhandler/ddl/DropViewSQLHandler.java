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
package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLDropViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import io.mycat.LockService;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Supplier;


public class DropViewSQLHandler extends AbstractSQLHandler<SQLDropViewStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropViewSQLHandler.class);
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLDropViewStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        SQLDropViewStatement ast = request.getAst();
        SQLExprTableSource sqlExprTableSource = ast.getTableSources().get(0);
        resolveSQLExprTableSource(sqlExprTableSource, dataContext);
        return lockService.lock(DDL_LOCK, new Supplier<Future<Void>>() {
            @Override
            public Future<Void> get() {
                    String schemaName = Optional.ofNullable(sqlExprTableSource.getSchema()).orElse(dataContext.getDefaultSchema());
                    schemaName = SQLUtils.normalize(schemaName);

                    String viewName = SQLUtils.normalize(sqlExprTableSource.getTableName());

                    try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                        ops.removeView(schemaName, viewName);
                        ops.commit();
                        return response.sendOk();
                    } catch (Throwable throwable) {
                        return Future.failedFuture(throwable);
                    }
            }
        });
    }
}

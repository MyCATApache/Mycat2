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
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.*;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.RenameMycatRowMetaData;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.HackRouter;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Pair;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


public class CreateViewSQLHandler extends AbstractSQLHandler<SQLCreateViewStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateViewSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLCreateViewStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        SQLCreateViewStatement ast = request.getAst();
        resolveSQLExprTableSource(ast.getTableSource(), dataContext);
        Future<Lock> lockFuture = lockService.getLock(DDL_LOCK);
        return lockFuture.flatMap(lock -> {
            try {
                String schemaName = Optional.ofNullable(ast.getSchema()).orElse(dataContext.getDefaultSchema());
                schemaName = SQLUtils.normalize(schemaName);

                String viewName = SQLUtils.normalize(ast.getName().getSimpleName());
                SQLSelect subQuery = ast.getSubQuery();
                SQLSelectStatement sqlSelectStatement = new SQLSelectStatement();
                sqlSelectStatement.setSelect(subQuery);

                List<String> aliasList = Optional.ofNullable(ast.getColumns()).orElse(Collections.emptyList()).stream().map(i -> SQLUtils.normalize(i.toString())).collect(Collectors.toList());

                HackRouter hackRouter = new HackRouter(sqlSelectStatement, dataContext);
                boolean distSql = !hackRouter.analyse();
                try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                    if (!distSql) {
                        Pair<String, String> plan = hackRouter.getPlan();
                        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                        try (DefaultConnection connection = jdbcConnectionManager.getConnection(plan.getKey())) {
                            Connection rawConnection = connection.getRawConnection();
                            Statement statement = rawConnection.createStatement();
                            statement.setMaxRows(0);
                            MycatRowMetaData metaData = connection.executeQuery(plan.getValue()).getMetaData();
                            if (!aliasList.isEmpty()) {
                                metaData = RenameMycatRowMetaData.of(metaData, aliasList);
                            }
                            String createTableSql = PrototypeService.generateSql(schemaName, viewName, metaData.metaData());
                            ops.putNormalTable(schemaName, viewName, (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(createTableSql));
                            statement.close();
                            try {
                                SQLSelectStatement phySQLSelectStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(plan.getValue());
                                ast.setSubQuery(phySQLSelectStatement.getSelect());
                                JdbcUtils.execute(rawConnection, ast.toString());//建立物理视图
                            } catch (Throwable throwable) {
                                LOGGER.error("build phy view fail", throwable);
                            }
                        }
                    } else {
                        ops.addView(schemaName, viewName, ast.toString());
                    }
                    ops.commit();
                } catch (Throwable throwable) {
                    return Future.failedFuture(throwable);
                }
                return response.sendOk();
            } finally {
                lock.release();
            }
        });
    }
}

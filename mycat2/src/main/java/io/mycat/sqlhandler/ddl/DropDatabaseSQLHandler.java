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
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import io.mycat.*;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;


public class DropDatabaseSQLHandler extends AbstractSQLHandler<SQLDropDatabaseStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropDatabaseSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLDropDatabaseStatement> request, MycatDataContext dataContext, Response response)  {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
       return lockService.lock(DDL_LOCK, new Supplier<Future<Void>>() {
           @Override
           public Future<Void> get() {
               SQLDropDatabaseStatement dropDatabaseStatement = request.getAst();
               String schemaName = SQLUtils.normalize(dropDatabaseStatement.getDatabaseName());
               try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                   ops.dropSchema(schemaName);
                   ops.commit();
                   onPhysics(schemaName);
                   return response.sendOk();
               }catch (Throwable throwable){
                   return Future.failedFuture(throwable);
               }
           }
       });
    }
    protected void onPhysics(String name) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(metadataManager.getPrototype())) {
            connection.executeUpdate(String.format(
                    "DROP DATABASE IF EXISTS %s;",
                    name),false);
        }catch (Throwable t){
            LOGGER.warn("",t);
        }
    }
}

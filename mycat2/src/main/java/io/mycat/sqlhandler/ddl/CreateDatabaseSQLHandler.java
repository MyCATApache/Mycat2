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
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import io.mycat.*;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.SqlHints;
import io.mycat.util.JsonUtil;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;


public class CreateDatabaseSQLHandler extends AbstractSQLHandler<SQLCreateDatabaseStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDatabaseSQLHandler.class);


    @Override
    protected Future<Void> onExecute(SQLRequest<SQLCreateDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        Future<Lock> lockFuture = lockService.getLockWithTimeout(DDL_LOCK);
       return lockFuture.flatMap(lock -> {
           SQLCreateDatabaseStatement ast = request.getAst();
           boolean ifNotExists = ast.isIfNotExists();
           String tableName = SQLUtils.normalize(ast.getName().getSimpleName());
           Map<String, Object> attributes = ast.getAttributes();
           String json = (String) attributes.get(SqlHints.AFTER_COMMENT);
           String targetName = JsonUtil.fromMap(json, "targetName").orElse(null);
           try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
               ops.addSchema(tableName, targetName);
               ops.commit();
               onPhysics(tableName);
               return response.sendOk();
           }catch (Throwable throwable){
               return Future.failedFuture(throwable);
           }finally {
               lock.release();
           }
       });
    }

    protected void onPhysics(String name) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(metadataManager.getPrototype())) {
            connection.executeUpdate(String.format(
                    "CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET utf8 COLLATE utf8_general_ci;",
                    name),false);
        }catch (Throwable t){
            LOGGER.warn("",t);
        }
    }
}

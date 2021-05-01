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
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

import java.util.Set;
import java.util.function.Function;

public class DropIndexSQLHandler extends AbstractSQLHandler<SQLDropIndexStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLDropIndexStatement> request, MycatDataContext dataContext, Response response){
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        Future<Lock> lockFuture = lockService.getLockWithTimeout(DDL_LOCK);
        return lockFuture.flatMap(lock -> {
            try{
                SQLDropIndexStatement sqlDropIndexStatement = request.getAst();
                SQLName indexName = sqlDropIndexStatement.getIndexName();
                resolveSQLExprTableSource(sqlDropIndexStatement.getTableName(), dataContext);
                SQLExprTableSource tableSource = sqlDropIndexStatement.getTableName();


                String schema = SQLUtils.normalize(tableSource.getSchema());
                String tableName = SQLUtils.normalize(tableSource.getTableName());
                MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                TableHandler table = metadataManager.getTable(schema, tableName);
                Set<DataNode> dataNodes = getDataNodes(table);
                dataNodes.add(new BackendTableInfo(metadataManager.getPrototype(),schema,tableName));//add Prototype
                executeOnDataNodes(sqlDropIndexStatement,jdbcConnectionManager,dataNodes,tableSource);
                return response.sendOk();
            }catch (Throwable throwable){
                return Future.failedFuture(throwable);
            }finally {
                lock.release();
            }
        });

    }
}

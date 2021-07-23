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
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;


public class AlterTableSQLHandler extends AbstractSQLHandler<SQLAlterTableStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlterTableSQLHandler.class);
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLAlterTableStatement> request, MycatDataContext dataContext, Response response){
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        Future<Lock> lockFuture = lockService.getLockWithTimeout(DDL_LOCK);
        return lockFuture.flatMap(lock -> {
            try {
                SQLAlterTableStatement sqlAlterTableStatement = request.getAst();
                SQLExprTableSource tableSource = sqlAlterTableStatement.getTableSource();
                resolveSQLExprTableSource(tableSource, dataContext);
                String schema = SQLUtils.normalize(sqlAlterTableStatement.getSchema());
                String tableName = SQLUtils.normalize(sqlAlterTableStatement.getTableName());
                MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                TableHandler tableHandler = metadataManager.getTable(schema, tableName);
                MySqlCreateTableStatement createTableStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(tableHandler.getCreateTableSQL());
                boolean changed = createTableStatement.apply(sqlAlterTableStatement);
                if (changed) {
                    JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                    Set<Partition> partitions = getDataNodes(tableHandler);
                    partitions.add(new BackendTableInfo(metadataManager.getPrototype(), schema, tableName));//add Prototype
                    executeOnDataNodes(sqlAlterTableStatement, connectionManager, partitions);
                    CreateTableSQLHandler.INSTANCE.createTable(Collections.emptyMap(), schema, tableName, createTableStatement);
                }
                return response.sendOk();
            }catch (Throwable throwable){
                return Future.failedFuture(throwable);
            }finally {
                lock.release();
            }
        });
    }


    public void executeOnDataNodes(SQLAlterTableStatement alterTableStatement,
                                   JdbcConnectionManager connectionManager,
                                   Collection<Partition> partitions) {
        SQLExprTableSource tableSource = alterTableStatement.getTableSource();
        executeOnDataNodes(alterTableStatement, connectionManager, partitions, tableSource);
    }

}

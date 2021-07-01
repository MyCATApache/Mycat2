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

import com.alibaba.druid.sql.MycatSQLUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;


public class CreateIndexSQLHandler extends AbstractSQLHandler<SQLCreateIndexStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLCreateIndexStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        Future<Lock> lockFuture = lockService.getLockWithTimeout(DDL_LOCK);
        return lockFuture.flatMap(lock -> {
            try {
                SQLCreateIndexStatement sqlCreateIndexStatement = request.getAst();
                SQLExprTableSource table = (SQLExprTableSource) sqlCreateIndexStatement.getTable();
                resolveSQLExprTableSource(table, dataContext);

                String schema = SQLUtils.normalize(sqlCreateIndexStatement.getSchema());
                String tableName = SQLUtils.normalize(sqlCreateIndexStatement.getTableName());
                MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

                if (!sqlCreateIndexStatement.isGlobal()) {
                    createLocalIndex(sqlCreateIndexStatement,
                            table,
                            schema,
                            tableName,
                            metadataManager);
                } else if (sqlCreateIndexStatement.getDbPartitionBy() != null) {
                    createGlobalIndex(sqlCreateIndexStatement);
                }
                return response.sendOk();
            } catch (Throwable throwable) {
                return response.sendError(throwable);
            } finally {
                lock.release();
            }
        });

    }

    private void createGlobalIndex(SQLCreateIndexStatement sqlCreateIndexStatement) throws Exception {
        SQLIndexDefinition indexDefinition = sqlCreateIndexStatement.getIndexDefinition();
        SQLExprTableSource table = (SQLExprTableSource) indexDefinition.getTable();
        String tableName = SQLUtils.normalize(table.getTableName());
        String schemaName = SQLUtils.normalize(table.getSchema());

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler tableHandler = Objects.requireNonNull(metadataManager.getTable(schemaName, tableName));
        MySqlCreateTableStatement createTableStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(tableHandler.getCreateTableSQL());
        MySqlTableIndex mySqlTableIndex = new MySqlTableIndex();
        indexDefinition.cloneTo(mySqlTableIndex.getIndexDefinition());

        //移除同名的索引
        createTableStatement.getTableElementList().removeAll(createTableStatement.getTableElementList().stream().filter(i -> {
            if (i instanceof MySqlTableIndex) {
                return mySqlTableIndex.getName().equals(((MySqlTableIndex) i).getIndexDefinition().getName());
            }
            return false;
        }).collect(Collectors.toList()));
        createTableStatement.getTableElementList().add(mySqlTableIndex);
        String s = MycatSQLUtils.toString(createTableStatement);
        CreateTableSQLHandler.INSTANCE.createTable(Collections.emptyMap(), schemaName, tableName, createTableStatement);
    }

    private void createLocalIndex(SQLCreateIndexStatement sqlCreateIndexStatement, SQLExprTableSource table, String schema, String tableName, MetadataManager metadataManager) {
        JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        TableHandler tableHandler = metadataManager.getTable(schema, tableName);
        Collection<Partition> partitions = getDataNodes(tableHandler);
        partitions.add(new BackendTableInfo(metadataManager.getPrototype(), schema, tableName));//add Prototype
        executeOnDataNodes(sqlCreateIndexStatement, connectionManager, partitions, table);
    }
}

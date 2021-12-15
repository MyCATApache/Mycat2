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
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.*;
import io.mycat.config.PrototypeServer;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class AlterTableSQLHandler extends AbstractSQLHandler<SQLAlterTableStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlterTableSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLAlterTableStatement> request, MycatDataContext dataContext, Response response) {
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
                JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                Set<Partition> partitions = getDataNodes(tableHandler);
                executeOnDataNodes(sqlAlterTableStatement, connectionManager, partitions);

                PrototypeService manager = MetaClusterCurrent.wrapper(PrototypeService.class);
                Optional<String> createTableSQLByJDBCOptional = manager.getCreateTableSQLByJDBC(schema, tableName, new ArrayList<>(partitions));
                if (createTableSQLByJDBCOptional.isPresent()) {
                    String createTableSQLByJDBC = createTableSQLByJDBCOptional.get();
                    MySqlCreateTableStatement oldCreateTableStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(tableHandler.getCreateTableSQL());
                    MySqlCreateTableStatement newMySqlCreateTableStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(createTableSQLByJDBC);
                    boolean broadCast = oldCreateTableStatement.isBroadCast();
                    SQLExpr dbPartitionBy = oldCreateTableStatement.getDbPartitionBy();
                    SQLExpr dbPartitions = oldCreateTableStatement.getDbPartitions();
                    SQLExpr tablePartitionBy = oldCreateTableStatement.getTablePartitionBy();
                    SQLExpr tbpartitions = oldCreateTableStatement.getTbpartitions();

                    newMySqlCreateTableStatement.setBroadCast(broadCast);
                    newMySqlCreateTableStatement.setDbPartitionBy(dbPartitionBy);
                    newMySqlCreateTableStatement.setDbPartitions(dbPartitions);
                    newMySqlCreateTableStatement.setTablePartitionBy(tablePartitionBy);
                    newMySqlCreateTableStatement.setTablePartitions(tbpartitions);


                    CreateTableSQLHandler.INSTANCE.createTable(Collections.emptyMap(), schema, tableName, newMySqlCreateTableStatement);
                } else {
                    return Future.failedFuture(new MycatException("can not generate new create table sql:" + sqlAlterTableStatement));
                }
                return response.sendOk();
            } catch (Throwable throwable) {
                return Future.failedFuture(throwable);
            } finally {
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

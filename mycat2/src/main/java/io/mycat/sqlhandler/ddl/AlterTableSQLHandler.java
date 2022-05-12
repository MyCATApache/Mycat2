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
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.GlobalTableConfig;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.config.NormalTableConfig;
import io.mycat.config.ShardingTableConfig;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.JsonUtil;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;


public class AlterTableSQLHandler extends AbstractSQLHandler<SQLAlterTableStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlterTableSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLAlterTableStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        return lockService.lock(DDL_LOCK, new Supplier<Future<Void>>() {
            @Override
            public Future<Void> get() {
                try {
                    SQLAlterTableStatement sqlAlterTableStatement = request.getAst();
                    sqlAlterTableStatement.setIfExists(true);
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
                        String createTableSQL = createTableSQLByJDBCOptional.get();
                        switch (tableHandler.getType()) {
                            case SHARDING: {
                                ShardingTable shardingTable = (ShardingTable) tableHandler;
                                ShardingTableConfig tableConfig = JsonUtil.clone(shardingTable.getTableConfig(),ShardingTableConfig.class);
                                try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                                    tableConfig.setCreateTableSQL(createTableSQL);
                                    ops.putShardingTable(schema, tableName, tableConfig);
                                    ops.commit();
                                }
                                break;
                            }
                            case GLOBAL: {
                                GlobalTable globalTable = (GlobalTable) tableHandler;
                                GlobalTableConfig tableConfig = JsonUtil.clone(globalTable.getTableConfig(), GlobalTableConfig.class);
                                tableConfig.setCreateTableSQL(createTableSQL);
                                try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                                    ops.putGlobalTableConfig(schema, tableName, tableConfig);
                                    ops.commit();
                                }
                                break;
                            }
                            case NORMAL: {
                                NormalTable normalTable = (NormalTable) tableHandler;
                                NormalTableConfig tableConfig = JsonUtil.clone(normalTable.getTableConfig(), NormalTableConfig.class);
                                tableConfig.setCreateTableSQL(createTableSQL);
                                try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                                    ops.putNormalTable(schema, tableName, tableConfig);
                                    ops.commit();
                                }
                                break;
                            }
                            case CUSTOM:
                                break;
                            case VISUAL:
                                break;
                            case VIEW:
                                break;
                        }
                    }
                    return response.sendOk();
                } catch (Throwable throwable) {
                    return Future.failedFuture(throwable);
                }
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

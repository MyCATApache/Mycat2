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
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import io.mycat.*;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.JsonUtil;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


public class RenameTableSQLHandler extends AbstractSQLHandler<MySqlRenameTableStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlRenameTableStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        Future<Lock> lockFuture = lockService.getLockWithTimeout(DDL_LOCK);
        return lockFuture.flatMap(lock -> {
            try {
                MySqlRenameTableStatement mySqlRenameTableStatement = request.getAst();
                for (MySqlRenameTableStatement.Item item : mySqlRenameTableStatement.getItems()) {
                    SQLName name = item.getName();
                    if (name instanceof SQLIdentifierExpr) {
                        checkDefaultSchemaNotNull(dataContext);
                        item.setName(new SQLPropertyExpr(dataContext.getDefaultSchema(), name.getSimpleName()));
                    }
                    SQLName to = item.getTo();
                    if (to instanceof SQLIdentifierExpr) {
                        checkDefaultSchemaNotNull(dataContext);
                        item.setTo(new SQLPropertyExpr(dataContext.getDefaultSchema(), to.getSimpleName()));
                    }
                }
                JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                for (MySqlRenameTableStatement.Item item : new ArrayList<>(mySqlRenameTableStatement.getItems())) {
                    MySqlRenameTableStatement sqlRenameTableStatement = cloneSql(mySqlRenameTableStatement);
                    sqlRenameTableStatement.getItems().clear();
                    sqlRenameTableStatement.addItem(item);

                    SQLPropertyExpr name = (SQLPropertyExpr) item.getName();
                    String orgSchemaName = name.getOwnerName();
                    String orgTableName = name.getName();
                    TableHandler tableHandler = metadataManager.getTable(orgSchemaName, orgTableName);

                    MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
                    Object tableConfig = mycatRouterConfig.getSchemas().stream()
                            .filter(n -> tableHandler.getSchemaName().equalsIgnoreCase(n.getSchemaName()))
                            .map(logicSchemaConfig -> {
                                NormalTableConfig normalTableConfig = logicSchemaConfig.getNormalTables().get(tableHandler.getTableName());
                                GlobalTableConfig globalTableConfig = logicSchemaConfig.getGlobalTables().get(tableHandler.getTableName());
                                ShardingTableConfig shardingTableConfig = logicSchemaConfig.getShadingTables().get(tableHandler.getTableName());
                                CustomTableConfig customTableConfig = logicSchemaConfig.getCustomTables().get(tableHandler.getTableName());
                                if (normalTableConfig != null) {
                                    return normalTableConfig;
                                }
                                if (globalTableConfig != null) {
                                    return globalTableConfig;
                                }
                                if (shardingTableConfig != null) {
                                    return shardingTableConfig;
                                }
                                if (customTableConfig != null) {
                                    return customTableConfig;
                                }
                                throw new IllegalArgumentException("unknown table:" + orgSchemaName + "." + orgTableName);
                            }).findFirst().orElseThrow(() -> new IllegalArgumentException("unknown table:" + orgSchemaName + "." + orgTableName));
                    Object newConfig = JsonUtil.from(JsonUtil.toJson(tableConfig), tableConfig.getClass());

                    SQLPropertyExpr to = (SQLPropertyExpr) item.getTo();
                    String newSchemaName = SQLUtils.normalize(to.getOwnerName());
                    String newTableName = SQLUtils.normalize(to.getName());

                    String createTableSQL = tableHandler.getCreateTableSQL();
                    MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(createTableSQL);

                    sqlStatement.setTableName(newTableName);
                    sqlStatement.setSchema(newSchemaName);

                    Set<DataNode> dataNodes = new HashSet<>();
                    dataNodes.add( new BackendTableInfo(metadataManager.getPrototype(), "", ""));

                    if (tableHandler.getType() == LogicTableType.GLOBAL){
                        dataNodes.addAll(getDataNodes(tableHandler));//更改所有节点
                    }
                    executeOnDataNodes(sqlRenameTableStatement, jdbcConnectionManager,dataNodes);

                    String newCreateTableSql = sqlStatement.toString();
                    try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                        ops.removeTable(orgSchemaName, orgTableName);
                        if (newConfig instanceof NormalTableConfig) {
                            NormalTableConfig normalTableConfig = (NormalTableConfig) newConfig;
                            normalTableConfig.setCreateTableSQL(newCreateTableSql);
                            ops.putNormalTable(newSchemaName, newTableName, normalTableConfig);
                        } else if (newConfig instanceof GlobalTableConfig) {
                            GlobalTableConfig globalTableConfig = (GlobalTableConfig) newConfig;
                            globalTableConfig.setCreateTableSQL(newCreateTableSql);
                            ops.putGlobalTableConfig(newSchemaName, newTableName, globalTableConfig);
                        } else if (newConfig instanceof ShardingTableConfig) {
                            ShardingTableConfig shardingTableConfig = (ShardingTableConfig) newConfig;
                            shardingTableConfig.setCreateTableSQL(newCreateTableSql);
                            ops.putShardingTable(newSchemaName, newTableName, shardingTableConfig);
                        } else if (newConfig instanceof CustomTableConfig) {
                            CustomTableConfig customTableConfig = (CustomTableConfig) newConfig;
                            customTableConfig.setCreateTableSQL(newCreateTableSql);
                            throw new UnsupportedOperationException();
                        }
                        ops.commit();
                    }
                }
                return response.sendOk();
            } catch (Throwable throwable) {
                return Future.failedFuture(throwable);
            }finally {
                lock.release();
            }
        });

    }

    private  static  MySqlRenameTableStatement cloneSql(MySqlRenameTableStatement mySqlRenameTableStatement) {
        return (MySqlRenameTableStatement)
                SQLUtils.parseSingleMysqlStatement(mySqlRenameTableStatement.toString());
    }

    public void executeOnDataNodes(MySqlRenameTableStatement sqlStatement,
                                   JdbcConnectionManager connectionManager,
                                   TableHandler tableHandler) {
        Collection<DataNode> dataNodes = getDataNodes(tableHandler);
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        dataNodes.add(new BackendTableInfo(metadataManager.getPrototype(),
                tableHandler.getSchemaName(), tableHandler.getTableName()));//add Prototype
        executeOnDataNodes(sqlStatement, connectionManager, dataNodes);
    }

    private void executeOnDataNodes(MySqlRenameTableStatement sqlStatement, JdbcConnectionManager connectionManager, Collection<DataNode> dataNodes) {
        for (DataNode dataNode : dataNodes) {
            MySqlRenameTableStatement each = cloneSql(sqlStatement);
            String sql = each.toString();
            try (DefaultConnection connection = connectionManager.getConnection(dataNode.getTargetName())) {
                connection.executeUpdate(sql, false);
            }
        }
    }

}

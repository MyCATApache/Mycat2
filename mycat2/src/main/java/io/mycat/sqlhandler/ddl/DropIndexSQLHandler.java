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
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.google.common.collect.ImmutableMap;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Set;

public class DropIndexSQLHandler extends AbstractSQLHandler<SQLDropIndexStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropIndexSQLHandler.class);
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLDropIndexStatement> request, MycatDataContext dataContext, Response response){
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        Future<Lock> lockFuture = lockService.getLock(DDL_LOCK);
        return lockFuture.flatMap(lock -> {
            try{
                SQLDropIndexStatement sqlDropIndexStatement = request.getAst();
                sqlDropIndexStatement.setIfExists(true);

                String indexName = SQLUtils.normalize(sqlDropIndexStatement.getIndexName().toString());
                resolveSQLExprTableSource(sqlDropIndexStatement.getTableName(), dataContext);
                SQLExprTableSource tableSource = sqlDropIndexStatement.getTableName();

                String schema = SQLUtils.normalize(tableSource.getSchema());
                String tableName = SQLUtils.normalize(tableSource.getTableName());
                MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);

                TableHandler table = metadataManager.getTable(schema, tableName);

                MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement)SQLUtils.parseSingleMysqlStatement(table.getCreateTableSQL());

                boolean updateShardingTable = false;
                updateShardingTable = isUpdateShardingTable(indexName, sqlStatement, updateShardingTable);
                if (!updateShardingTable){
                    Set<Partition> partitions = getDataNodes(table);
                    try {
                        executeOnDataNodes(sqlDropIndexStatement, jdbcConnectionManager, partitions, tableSource);
                    }catch (Throwable e){
                        LOGGER.error("",e);
                    }
                }
//                List<MySqlTableIndex> mysqlIndexes = sqlStatement.getMysqlIndexes();
//                mysqlIndexes.stream().filter(i->SQLUtils.nameEquals(sqlDropIndexStatement.getIndexName(),i.getName())).findFirst()
//                        .ifPresent(c->mysqlIndexes.remove(c));
                sqlStatement.apply(sqlDropIndexStatement);

                CreateTableSQLHandler.INSTANCE.createTable(ImmutableMap.of(),schema,tableName,sqlStatement);
                return response.sendOk();
            }catch (Throwable throwable){
                return Future.failedFuture(throwable);
            }finally {
                lock.release();
            }
        });

    }

    private boolean isUpdateShardingTable(String indexName, MySqlCreateTableStatement sqlStatement, boolean updateShardingTable) {
        for (SQLTableElement c : new ArrayList<>(sqlStatement.getTableElementList())) {
            if (c instanceof MySqlTableIndex) {
                MySqlTableIndex mySqlTableIndex = (MySqlTableIndex) c;
                String normalize = SQLUtils.normalize(mySqlTableIndex.getName().toString());
                if (normalize.equals(indexName)){
                    if(mySqlTableIndex.getDbPartitionBy()!=null){
                        sqlStatement.getTableElementList().remove(mySqlTableIndex);
                        updateShardingTable = true;
                    }
                }
            }
        }
        return updateShardingTable;
    }
}

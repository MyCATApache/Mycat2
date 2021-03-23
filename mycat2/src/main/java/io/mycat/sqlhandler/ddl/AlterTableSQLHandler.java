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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;


public class AlterTableSQLHandler extends AbstractSQLHandler<SQLAlterTableStatement> {

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
                    Set<DataNode> dataNodes = getDataNodes(tableHandler);
                    dataNodes.add(new BackendTableInfo(metadataManager.getPrototype(), schema, tableName));//add Prototype
                    executeOnDataNodes(sqlAlterTableStatement, connectionManager, dataNodes);
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
                                   Collection<DataNode> dataNodes) {
        SQLExprTableSource tableSource = alterTableStatement.getTableSource();
        executeOnDataNodes(alterTableStatement, connectionManager, dataNodes, tableSource);
    }

}

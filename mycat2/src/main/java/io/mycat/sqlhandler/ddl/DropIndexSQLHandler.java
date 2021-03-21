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
        Future<Lock> lockFuture = lockService.getLockWithTimeout(getClass().getName());
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

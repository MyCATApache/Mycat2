package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.util.List;


public class CreateIndexSQLHandler extends AbstractSQLHandler<SQLCreateIndexStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLCreateIndexStatement> request, MycatDataContext dataContext, Response response) {
        try{
            SQLCreateIndexStatement sqlCreateIndexStatement = request.getAst();
            SQLExprTableSource table = (SQLExprTableSource)sqlCreateIndexStatement.getTable();
            resolveSQLExprTableSource(table,dataContext);

            String schema = SQLUtils.normalize(sqlCreateIndexStatement.getSchema());
            String tableName = SQLUtils.normalize(sqlCreateIndexStatement.getTableName());
            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

            if(!sqlCreateIndexStatement.isGlobal()){
                createLocalIndex(sqlCreateIndexStatement,
                        table,
                        schema,
                        tableName,
                        metadataManager);
            }else {
                createGlobalIndex(sqlCreateIndexStatement);
            }
            return response.sendOk();
        }catch (Throwable throwable){
            return response.sendError(throwable);
        }
    }

    private void createGlobalIndex(SQLCreateIndexStatement sqlCreateIndexStatement) {
        SQLIndexDefinition indexDefinition = sqlCreateIndexStatement.getIndexDefinition();
        //todo
    }

    private void createLocalIndex(SQLCreateIndexStatement sqlCreateIndexStatement, SQLExprTableSource table, String schema, String tableName, MetadataManager metadataManager) {
        JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        TableHandler tableHandler = metadataManager.getTable(schema, tableName);
        List<DataNode> dataNodes = getDataNodes(tableHandler);
        executeOnPrototype(sqlCreateIndexStatement,connectionManager);
        executeOnDataNodes(sqlCreateIndexStatement,connectionManager,dataNodes,table);
    }
}

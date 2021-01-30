package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDDLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.impl.future.PromiseInternal;

import java.util.Collections;
import java.util.List;


public class AlterTableSQLHandler extends AbstractSQLHandler<SQLAlterTableStatement> {

    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<SQLAlterTableStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLAlterTableStatement sqlAlterTableStatement = request.getAst();
        SQLExprTableSource tableSource = sqlAlterTableStatement.getTableSource();
        resolveSQLExprTableSource(tableSource,dataContext);
        String schema = SQLUtils.normalize(sqlAlterTableStatement.getSchema());
        String tableName = SQLUtils.normalize(sqlAlterTableStatement.getTableName());
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler tableHandler = metadataManager.getTable(schema, tableName);
        MySqlCreateTableStatement createTableStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(tableHandler.getCreateTableSQL());
        boolean changed = createTableStatement.apply(sqlAlterTableStatement);
        if (changed) {
            JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            List<DataNode> dataNodes;
            dataNodes = getDataNodes(tableHandler);
            executeOnPrototype(sqlAlterTableStatement,connectionManager);
            executeOnDataNodes(sqlAlterTableStatement, connectionManager, dataNodes);
            CreateTableSQLHandler.INSTANCE.createTable(Collections.emptyMap(),schema,tableName,createTableStatement);
        }
        return response.sendOk();
    }


    public void executeOnDataNodes(SQLAlterTableStatement alterTableStatement,
                                   JdbcConnectionManager connectionManager,
                                   List<DataNode> dataNodes) {
        SQLExprTableSource tableSource = alterTableStatement.getTableSource();
        executeOnDataNodes(alterTableStatement, connectionManager, dataNodes, tableSource);
    }

}

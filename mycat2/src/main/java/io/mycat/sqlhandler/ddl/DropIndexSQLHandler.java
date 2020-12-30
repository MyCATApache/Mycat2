package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;

public class DropIndexSQLHandler extends AbstractSQLHandler<SQLDropIndexStatement> {
    @Override
    protected void onExecute(SQLRequest<SQLDropIndexStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLDropIndexStatement sqlDropIndexStatement = request.getAst();
        SQLName indexName = sqlDropIndexStatement.getIndexName();
        resolveSQLExprTableSource(sqlDropIndexStatement.getTableName(), dataContext);
        SQLExprTableSource tableSource = sqlDropIndexStatement.getTableName();


        String schema = SQLUtils.normalize(tableSource.getSchema());
        String tableName = SQLUtils.normalize(tableSource.getTableName());
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        TableHandler table = metadataManager.getTable(schema, tableName);
        executeOnPrototype(sqlDropIndexStatement,jdbcConnectionManager);
        executeOnDataNodes(sqlDropIndexStatement,jdbcConnectionManager,getDataNodes(table),tableSource);
        response.sendOk();
        return;
    }
}

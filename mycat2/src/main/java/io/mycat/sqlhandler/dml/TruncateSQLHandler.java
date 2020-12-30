package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLTruncateStatement;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;

import java.util.ArrayList;
import java.util.List;

import static io.mycat.sqlhandler.dml.UpdateSQLHandler.updateHandler;


public class TruncateSQLHandler extends AbstractSQLHandler<SQLTruncateStatement> {


    @Override
    protected void onExecute(SQLRequest<SQLTruncateStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLTruncateStatement truncateStatement = request.getAst();

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);


        for (SQLExprTableSource source : new ArrayList<>(truncateStatement.getTableSources())) {
            resolveSQLExprTableSource(source,dataContext);
            SQLTruncateStatement eachTruncateStatement   = clone(truncateStatement);
            eachTruncateStatement.getTableSources().clear();
            eachTruncateStatement.addTableSource(source.getName());


            TableHandler table = metadataManager.getTable(source.getSchema(), source.getTableName());

            List<DataNode> dataNodes = getDataNodes(table);

            executeOnPrototype(eachTruncateStatement,jdbcConnectionManager);
            executeOnDataNodes(eachTruncateStatement,jdbcConnectionManager,dataNodes);
        }
        response.sendOk();
    }

    private SQLTruncateStatement clone(SQLTruncateStatement truncateStatement) {
        return (SQLTruncateStatement)SQLUtils.parseSingleMysqlStatement(truncateStatement.toString());
    }

    public void executeOnDataNodes(SQLTruncateStatement truncateStatement,
                                   JdbcConnectionManager connectionManager,
                                   List<DataNode> dataNodes) {
        SQLExprTableSource tableSource = truncateStatement.getTableSources().get(0);
        executeOnDataNodes(truncateStatement, connectionManager, dataNodes, tableSource);
    }

}

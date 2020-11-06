package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLReplaceStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;



import static io.mycat.sqlhandler.dml.UpdateSQLHandler.updateHandler;


public class ReplaceSQLHandler extends AbstractSQLHandler<SQLReplaceStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLReplaceStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLExprTableSource tableSource = request.getAst().getTableSource();
        updateHandler(request.getAst(),dataContext,tableSource,response);
    }
}

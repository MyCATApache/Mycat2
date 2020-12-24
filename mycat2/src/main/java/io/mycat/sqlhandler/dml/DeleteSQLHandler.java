package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;


import static io.mycat.sqlhandler.dml.UpdateSQLHandler.updateHandler;


public class DeleteSQLHandler extends AbstractSQLHandler<MySqlDeleteStatement> {

    @Override
    protected void onExecute(SQLRequest<MySqlDeleteStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLExprTableSource tableSource = (SQLExprTableSource)request.getAst().getTableSource();
        updateHandler(request.getAst(),dataContext,tableSource,response);
    }
}

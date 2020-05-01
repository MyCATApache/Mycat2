package io.mycat.sqlHandler.dml;

import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import io.mycat.ExplainResponse;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

import static io.mycat.sqlHandler.dml.UpdateSQLHandler.updateHandler;

@Resource
public class DeleteSQLHandler extends AbstractSQLHandler<MySqlDeleteStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlDeleteStatement> request, MycatDataContext dataContext, Response response) {
        SQLExprTableSource tableSource = (SQLExprTableSource)request.getAst().getTableSource();
        updateHandler(request.getAst(),dataContext,tableSource,response);
        return ExecuteCode.PERFORMED;
    }
    @Override
    public ExecuteCode explain(SQLRequest<MySqlDeleteStatement> request, MycatDataContext dataContext, Response response) {
        SQLExprTableSource tableSource = (SQLExprTableSource)request.getAst().getTableSource();
        updateHandler(request.getAst(), dataContext, (SQLExprTableSource) tableSource,new ExplainResponse(DeleteSQLHandler.class,response) );
        return ExecuteCode.PERFORMED;
    }
}

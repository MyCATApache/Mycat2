package io.mycat.sqlHandler.dml;

import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

import static io.mycat.sqlHandler.dml.UpdateSQLHandler.updateHandler;

@Resource
public class InsertSQLHandler extends AbstractSQLHandler<MySqlInsertStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlInsertStatement> request, MycatDataContext dataContext, Response response) {
        SQLExprTableSource tableSource = request.getAst().getTableSource();
        updateHandler(request.getAst(),dataContext,tableSource,response);
        return ExecuteCode.PERFORMED;
    }
    @Override
    public ExecuteCode onExplain(SQLRequest<MySqlInsertStatement> request, MycatDataContext dataContext, Response response) {
        response.setExplainMode(true);
        SQLExprTableSource tableSource = (SQLExprTableSource)request.getAst().getTableSource();
        updateHandler(request.getAst(), dataContext, (SQLExprTableSource) tableSource,response);
        return ExecuteCode.PERFORMED;
    }
}

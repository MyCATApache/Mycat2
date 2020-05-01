package io.mycat.sqlHandler.dml;

import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLTruncateStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

import static io.mycat.sqlHandler.dml.UpdateSQLHandler.updateHandler;

@Resource
public class TruncateSQLHandler extends AbstractSQLHandler<SQLTruncateStatement> {


    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLTruncateStatement> request, MycatDataContext dataContext, Response response) {
        SQLExprTableSource tableSource = request.getAst().getTableSources().get(0);
        updateHandler(request.getAst(), dataContext, tableSource, response);
        return ExecuteCode.PERFORMED;
    }
}

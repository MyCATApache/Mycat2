package io.mycat.sqlhandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLUseStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;

public class UseSQLHandler extends AbstractSQLHandler<com.alibaba.fastsql.sql.ast.statement.SQLUseStatement> {
    @Override
    protected ExecuteCode onExecute(SQLRequest<com.alibaba.fastsql.sql.ast.statement.SQLUseStatement> request, MycatDataContext dataContext, Response response) {
        SQLUseStatement statement = request.getAst();
        String simpleName = statement.getDatabase().getSimpleName();
        dataContext.useShcema(simpleName);
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}
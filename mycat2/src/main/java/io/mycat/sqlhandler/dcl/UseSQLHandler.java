package io.mycat.sqlhandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLUseStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;

public class UseSQLHandler extends AbstractSQLHandler<com.alibaba.fastsql.sql.ast.statement.SQLUseStatement> {
    @Override
    protected void onExecute(SQLRequest<com.alibaba.fastsql.sql.ast.statement.SQLUseStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLUseStatement statement = request.getAst();
        String simpleName = statement.getDatabase().getSimpleName();
        dataContext.useShcema(simpleName);
        response.sendOk();
    }
}
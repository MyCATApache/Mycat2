package io.mycat.sqlHandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLUseStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;

public class UseSQLHandler extends AbstractSQLHandler<com.alibaba.fastsql.sql.ast.statement.SQLUseStatement> {
    @Override
    protected ExecuteCode onExecute(SQLRequest<com.alibaba.fastsql.sql.ast.statement.SQLUseStatement> request, MycatDataContext dataContext, Response response) {
        SQLUseStatement statement = request.getAst();
        String simpleName = statement.getDatabase().getSimpleName();
        MycatDBClientMediator client = MycatDBs.createClient(dataContext);
        client.useSchema(simpleName);
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}
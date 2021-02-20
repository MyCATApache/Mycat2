package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;

public class UseSQLHandler extends AbstractSQLHandler<com.alibaba.druid.sql.ast.statement.SQLUseStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLUseStatement> request, MycatDataContext dataContext, Response response){
        SQLUseStatement statement = request.getAst();
        String simpleName = statement.getDatabase().getSimpleName();
        dataContext.useShcema(simpleName);
        return response.sendOk();
    }
}
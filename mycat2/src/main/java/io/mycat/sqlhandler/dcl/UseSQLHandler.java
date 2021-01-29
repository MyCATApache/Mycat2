package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.impl.future.PromiseInternal;

public class UseSQLHandler extends AbstractSQLHandler<com.alibaba.druid.sql.ast.statement.SQLUseStatement> {
    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<com.alibaba.druid.sql.ast.statement.SQLUseStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLUseStatement statement = request.getAst();
        String simpleName = statement.getDatabase().getSimpleName();
        dataContext.useShcema(simpleName);
        return response.sendOk();
    }
}
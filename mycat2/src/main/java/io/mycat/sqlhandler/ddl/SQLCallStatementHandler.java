package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateProcedureStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

public class SQLCallStatementHandler extends AbstractSQLHandler<SQLCallStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLCallStatement> request, MycatDataContext dataContext, Response response) {
        SQLCallStatement ast = request.getAst();

        return response.sendOk();
    }
}

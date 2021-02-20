package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class CreateViewSQLHandler extends AbstractSQLHandler<SQLCreateViewStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLCreateViewStatement> request, MycatDataContext dataContext, Response response) {
        return response.sendOk();
    }
}

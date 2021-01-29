package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.impl.future.PromiseInternal;


public class CreateViewSQLHandler extends AbstractSQLHandler<SQLCreateViewStatement> {

    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<SQLCreateViewStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        return response.sendOk();
    }
}

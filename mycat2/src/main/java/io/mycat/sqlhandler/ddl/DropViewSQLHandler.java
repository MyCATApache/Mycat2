package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.statement.SQLDropViewStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.impl.future.PromiseInternal;


public class DropViewSQLHandler extends AbstractSQLHandler<SQLDropViewStatement> {

    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<SQLDropViewStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        return response.sendOk();
    }
}

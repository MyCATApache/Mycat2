package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.ast.statement.SQLStartTransactionStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.impl.future.PromiseInternal;


public class StartTransactionSQLHandler extends AbstractSQLHandler<SQLStartTransactionStatement> {

    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<SQLStartTransactionStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        return response.begin();
    }
}

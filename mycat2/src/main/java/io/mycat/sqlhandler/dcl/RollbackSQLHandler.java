package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.impl.future.PromiseInternal;


public class RollbackSQLHandler extends AbstractSQLHandler<SQLRollbackStatement> {

    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<SQLRollbackStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        return response.rollback();
    }
}

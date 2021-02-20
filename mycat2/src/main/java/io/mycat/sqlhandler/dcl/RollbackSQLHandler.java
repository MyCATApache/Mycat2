package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class RollbackSQLHandler extends AbstractSQLHandler<SQLRollbackStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLRollbackStatement> request, MycatDataContext dataContext, Response response) {
        return response.rollback();
    }
}

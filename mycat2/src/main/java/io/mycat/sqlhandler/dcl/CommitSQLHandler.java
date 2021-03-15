package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.ast.statement.SQLCommitStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class CommitSQLHandler extends AbstractSQLHandler<SQLCommitStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLCommitStatement> request, MycatDataContext dataContext, Response response) {
        return response.commit();
    }
}

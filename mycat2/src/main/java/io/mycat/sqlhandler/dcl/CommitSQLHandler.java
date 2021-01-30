package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.ast.statement.SQLCommitStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.impl.future.PromiseInternal;


public class CommitSQLHandler extends AbstractSQLHandler<SQLCommitStatement> {

    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<SQLCommitStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        return response.commit();
    }
}

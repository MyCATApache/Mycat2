package io.mycat.sqlhandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLCommitStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class CommitSQLHandler extends AbstractSQLHandler<SQLCommitStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLCommitStatement> request, MycatDataContext dataContext, Response response) {
        response.commit();
    }
}

package io.mycat.sqlHandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLCommitStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class CommitSQLHandler extends AbstractSQLHandler<SQLCommitStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLCommitStatement> request, MycatDataContext dataContext, Response response) {
        response.commit();
        return ExecuteCode.PERFORMED;
    }
}

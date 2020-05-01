package io.mycat.sqlHandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLRollbackStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class RollbackSQLHandler extends AbstractSQLHandler<SQLRollbackStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLRollbackStatement> request, MycatDataContext dataContext, Response response) {
        response.rollback();
        return ExecuteCode.PERFORMED;
    }
}

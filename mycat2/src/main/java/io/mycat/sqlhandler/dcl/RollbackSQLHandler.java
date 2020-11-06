package io.mycat.sqlhandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLRollbackStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class RollbackSQLHandler extends AbstractSQLHandler<SQLRollbackStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLRollbackStatement> request, MycatDataContext dataContext, Response response) {
        response.rollback();
    }
}

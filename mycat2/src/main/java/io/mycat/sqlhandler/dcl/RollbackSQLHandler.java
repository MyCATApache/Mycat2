package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;




public class RollbackSQLHandler extends AbstractSQLHandler<SQLRollbackStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLRollbackStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.rollback();
    }
}

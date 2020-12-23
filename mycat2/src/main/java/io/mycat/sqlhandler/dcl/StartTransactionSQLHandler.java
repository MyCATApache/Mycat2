package io.mycat.sqlhandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLStartTransactionStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;




public class StartTransactionSQLHandler extends AbstractSQLHandler<SQLStartTransactionStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLStartTransactionStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.begin();
    }
}

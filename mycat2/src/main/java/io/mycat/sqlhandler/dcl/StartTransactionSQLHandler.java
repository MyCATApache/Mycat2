package io.mycat.sqlhandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLStartTransactionStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class StartTransactionSQLHandler extends AbstractSQLHandler<SQLStartTransactionStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLStartTransactionStatement> request, MycatDataContext dataContext, Response response) {
        response.begin();
        return ExecuteCode.PERFORMED;
    }
}

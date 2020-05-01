package io.mycat.sqlHandler.dcl;

import com.alibaba.fastsql.sql.ast.statement.SQLStartTransactionStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class StartTransactionSQLHandler extends AbstractSQLHandler<SQLStartTransactionStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLStartTransactionStatement> request, MycatDataContext dataContext, Response response) {
        response.begin();
        return ExecuteCode.PERFORMED;
    }
}

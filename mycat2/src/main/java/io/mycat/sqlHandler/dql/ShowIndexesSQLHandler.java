package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.ast.statement.SQLShowIndexesStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;




public class ShowIndexesSQLHandler extends AbstractSQLHandler<SQLShowIndexesStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLShowIndexesStatement> request, MycatDataContext dataContext, Response response) {
        response.proxyShow(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

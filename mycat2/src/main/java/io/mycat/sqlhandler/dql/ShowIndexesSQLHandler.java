package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.ast.statement.SQLShowIndexesStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ShowIndexesSQLHandler extends AbstractSQLHandler<SQLShowIndexesStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLShowIndexesStatement> request, MycatDataContext dataContext, Response response) {
        response.proxyShow(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

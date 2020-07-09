package io.mycat.sqlhandler.dql;

import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ShowCreateFunctionHanlder  extends AbstractSQLHandler<com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement> request, MycatDataContext dataContext, Response response) {
        response.tryBroadcast(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

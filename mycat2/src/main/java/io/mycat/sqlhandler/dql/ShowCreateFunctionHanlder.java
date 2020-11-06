package io.mycat.sqlhandler.dql;

import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ShowCreateFunctionHanlder  extends AbstractSQLHandler<com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement> {

    @Override
    protected void onExecute(SQLRequest<com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.tryBroadcastShow(request.getAst().toString());
    }
}

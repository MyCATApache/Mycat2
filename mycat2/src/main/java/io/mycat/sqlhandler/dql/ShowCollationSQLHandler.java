package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ShowCollationSQLHandler extends AbstractSQLHandler<MySqlShowCollationStatement> {


    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlShowCollationStatement> request, MycatDataContext dataContext, Response response) {
        response.tryBroadcast(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

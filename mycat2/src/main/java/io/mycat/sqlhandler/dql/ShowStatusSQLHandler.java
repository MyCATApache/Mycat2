package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ShowStatusSQLHandler extends AbstractSQLHandler<MySqlShowStatusStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlShowStatusStatement> request, MycatDataContext dataContext, Response response) {
        response.tryBroadcast(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

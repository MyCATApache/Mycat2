package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowDatabaseStatusStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ShowDatabaseStatusSQLHandler extends AbstractSQLHandler<MySqlShowDatabaseStatusStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlShowDatabaseStatusStatement> request, MycatDataContext dataContext, Response response) {
        response.tryBroadcast(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

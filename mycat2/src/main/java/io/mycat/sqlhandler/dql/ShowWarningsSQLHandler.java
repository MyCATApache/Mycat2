package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ShowWarningsSQLHandler extends AbstractSQLHandler<MySqlShowWarningsStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlShowWarningsStatement> request, MycatDataContext dataContext, Response response) {
        response.proxyShow(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowErrorsStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;




public class ShowErrorsSQLHandler extends AbstractSQLHandler<MySqlShowErrorsStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlShowErrorsStatement> request, MycatDataContext dataContext, Response response) {
        response.proxyShow(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

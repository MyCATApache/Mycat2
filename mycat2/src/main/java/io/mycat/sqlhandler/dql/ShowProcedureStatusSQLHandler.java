package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowProcedureStatusStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ShowProcedureStatusSQLHandler extends AbstractSQLHandler<MySqlShowProcedureStatusStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlShowProcedureStatusStatement> request, MycatDataContext dataContext, Response response) {
        response.tryBroadcast(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

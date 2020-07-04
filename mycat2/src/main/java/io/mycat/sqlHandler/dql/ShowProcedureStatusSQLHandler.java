package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowProcedureStatusStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;




public class ShowProcedureStatusSQLHandler extends AbstractSQLHandler<MySqlShowProcedureStatusStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlShowProcedureStatusStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}

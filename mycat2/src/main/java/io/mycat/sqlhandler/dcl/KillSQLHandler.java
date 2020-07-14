package io.mycat.sqlhandler.dcl;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class KillSQLHandler extends AbstractSQLHandler<MySqlKillStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlKillStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}

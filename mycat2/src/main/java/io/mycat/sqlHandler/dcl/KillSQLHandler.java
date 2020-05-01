package io.mycat.sqlHandler.dcl;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class KillSQLHandler extends AbstractSQLHandler<MySqlKillStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlKillStatement> request, MycatDataContext dataContext, Response response) {
        return ExecuteCode.PERFORMED;
    }
}

package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLCreateDatabaseStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class CreateDatabaseSQLHandler extends AbstractSQLHandler<SQLCreateDatabaseStatement> {


    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLCreateDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}

package io.mycat.sqlHandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLDropTableStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;




public class DropTableSQLHandler extends AbstractSQLHandler<SQLDropTableStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLDropTableStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}

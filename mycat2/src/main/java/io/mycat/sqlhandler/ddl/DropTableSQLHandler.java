package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLDropTableStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class DropTableSQLHandler extends AbstractSQLHandler<SQLDropTableStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLDropTableStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}

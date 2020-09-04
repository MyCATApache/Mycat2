package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLDropDatabaseStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class DropDatabaseSQLHandler extends AbstractSQLHandler<SQLDropDatabaseStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLDropDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
    }
}

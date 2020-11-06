package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLAlterDatabaseStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;


public class AlterDatabaseSQLHandler extends AbstractSQLHandler<SQLAlterDatabaseStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLAlterDatabaseStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.sendOk();
    }
}

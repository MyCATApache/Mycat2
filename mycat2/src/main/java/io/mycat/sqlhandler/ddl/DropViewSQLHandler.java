package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLDropViewStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class DropViewSQLHandler extends AbstractSQLHandler<SQLDropViewStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLDropViewStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.sendOk();
    }
}

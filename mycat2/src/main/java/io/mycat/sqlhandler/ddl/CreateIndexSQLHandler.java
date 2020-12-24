package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLCreateIndexStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;




public class CreateIndexSQLHandler extends AbstractSQLHandler<SQLCreateIndexStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLCreateIndexStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.sendOk();
    }
}

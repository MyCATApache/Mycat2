package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.ast.statement.SQLShowDatabasesStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ShowDatabaseSQLHandler extends AbstractSQLHandler<SQLShowDatabasesStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLShowDatabasesStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.tryBroadcastShow(request.getSqlString());
    }
}

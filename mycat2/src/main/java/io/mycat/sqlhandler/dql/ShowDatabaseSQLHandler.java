package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;




public class ShowDatabaseSQLHandler extends AbstractSQLHandler<SQLShowDatabasesStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLShowDatabasesStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.proxySelectToPrototype(request.getSqlString());
    }
}

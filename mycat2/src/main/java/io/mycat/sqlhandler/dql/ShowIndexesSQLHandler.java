package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.statement.SQLShowIndexesStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;




public class ShowIndexesSQLHandler extends AbstractSQLHandler<SQLShowIndexesStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLShowIndexesStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.proxySelectToPrototype(request.getAst().toString());
    }
}

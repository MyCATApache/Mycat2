package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class ShowDatabaseSQLHandler extends AbstractSQLHandler<SQLShowDatabasesStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowDatabasesStatement> request, MycatDataContext dataContext, Response response){
        return response.proxySelectToPrototype(request.getSqlString());
    }
}

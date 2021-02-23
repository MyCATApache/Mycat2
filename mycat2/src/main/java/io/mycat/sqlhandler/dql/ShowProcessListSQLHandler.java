package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class ShowProcessListSQLHandler extends AbstractSQLHandler<MySqlShowProcessListStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowProcessListStatement> request, MycatDataContext dataContext, Response response){
        return response.proxySelectToPrototype(request.getAst().toString());
    }
}

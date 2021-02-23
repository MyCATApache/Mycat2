package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabaseStatusStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class ShowDatabaseStatusSQLHandler extends AbstractSQLHandler<MySqlShowDatabaseStatusStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowDatabaseStatusStatement> request, MycatDataContext dataContext, Response response){
        return response.proxySelectToPrototype(request.getSqlString());
    }
}

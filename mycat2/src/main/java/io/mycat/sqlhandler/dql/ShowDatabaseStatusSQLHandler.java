package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowDatabaseStatusStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;




public class ShowDatabaseStatusSQLHandler extends AbstractSQLHandler<MySqlShowDatabaseStatusStatement> {

    @Override
    protected void onExecute(SQLRequest<MySqlShowDatabaseStatusStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.tryBroadcastShow(request.getSqlString());
    }
}

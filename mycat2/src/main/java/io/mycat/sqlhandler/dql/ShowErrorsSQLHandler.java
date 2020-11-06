package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowErrorsStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ShowErrorsSQLHandler extends AbstractSQLHandler<MySqlShowErrorsStatement> {

    @Override
    protected void onExecute(SQLRequest<MySqlShowErrorsStatement> request, MycatDataContext dataContext, Response response) {
        response.tryBroadcastShow(request.getSqlString());
    }
}

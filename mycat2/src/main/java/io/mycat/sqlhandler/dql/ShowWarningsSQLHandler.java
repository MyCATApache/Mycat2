package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;




public class ShowWarningsSQLHandler extends AbstractSQLHandler<MySqlShowWarningsStatement> {

    @Override
    protected void onExecute(SQLRequest<MySqlShowWarningsStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.proxySelectToPrototype(request.getAst().toString());
    }
}

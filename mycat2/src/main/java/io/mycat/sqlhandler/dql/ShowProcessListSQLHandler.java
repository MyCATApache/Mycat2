package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;




public class ShowProcessListSQLHandler extends AbstractSQLHandler<MySqlShowProcessListStatement> {

    @Override
    protected void onExecute(SQLRequest<MySqlShowProcessListStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.proxySelectToPrototype(request.getAst().toString());
        return ;
    }
}

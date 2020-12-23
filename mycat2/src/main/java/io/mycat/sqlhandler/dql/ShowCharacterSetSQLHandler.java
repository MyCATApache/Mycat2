package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;




public class ShowCharacterSetSQLHandler extends AbstractSQLHandler<MySqlShowCharacterSetStatement> {

    @Override
    protected void onExecute(SQLRequest<MySqlShowCharacterSetStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.tryBroadcastShow(request.getAst().toString());
    }
}

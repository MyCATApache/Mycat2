package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatdbCommand;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class ExplainSQLHandler extends AbstractSQLHandler<MySqlExplainStatement> {


    @Override
    protected void onExecute(SQLRequest<MySqlExplainStatement> request, MycatDataContext dataContext, Response response) {
        MySqlExplainStatement ast = request.getAst();
        if(ast.isDescribe()){
            response.tryBroadcastShow(ast.toString());
            return ;
        }
      throw new UnsupportedOperationException();
    }
}

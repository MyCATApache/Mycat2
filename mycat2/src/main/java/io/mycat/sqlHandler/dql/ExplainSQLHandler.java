package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatdbCommand;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class ExplainSQLHandler extends AbstractSQLHandler<MySqlExplainStatement> {


    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlExplainStatement> request, MycatDataContext dataContext, Response response) {
        MySqlExplainStatement ast = request.getAst();
        if(ast.isDescribe()){
            response.proxyShow(ast);
            return ExecuteCode.PERFORMED;
        }
        SQLStatement statement = ast.getStatement();
        MycatRequest mycatRequest = request.getRequest();
        MycatRequest request1 = MycatRequest.builder()
                .sessionId(mycatRequest.getSessionId())
                .text(statement.toString())
                .context(mycatRequest.getContext())
                .userSpace(mycatRequest.getUserSpace()).build();
        MycatdbCommand.INSTANCE.explain(request1,dataContext,response);
        return ExecuteCode.PERFORMED;
    }
}

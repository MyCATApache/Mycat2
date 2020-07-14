package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatdbCommand;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;

public class HintSQLHandler extends AbstractSQLHandler<MySqlHintStatement> {
    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlHintStatement> request, MycatDataContext dataContext, Response response) {
        MycatRequest mycatRequest = request.getRequest();
        MycatRequest newMycatRequest = MycatRequest.builder()
                .context(mycatRequest.getContext())
                .sessionId(mycatRequest.getSessionId())
                .text(request.getAst().getHintStatements().get(0).toString())
                .userSpace(mycatRequest.getUserSpace())
                .build();
        MycatdbCommand.INSTANCE.run(newMycatRequest,dataContext,response);
        return ExecuteCode.PERFORMED;
    }
}
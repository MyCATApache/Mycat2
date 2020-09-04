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
    protected void onExecute(SQLRequest<MySqlHintStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
    }
}
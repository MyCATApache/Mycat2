package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLDropTableStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatRouterConfigOps;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.dql.HintSQLHandler;
import io.mycat.util.JsonUtil;
import io.mycat.util.Response;

import java.util.Map;


public class DropTableSQLHandler extends AbstractSQLHandler<SQLDropTableStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLDropTableStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
    }
}

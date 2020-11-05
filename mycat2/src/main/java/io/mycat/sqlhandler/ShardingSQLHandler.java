package io.mycat.sqlhandler;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.MycatDataContext;
import io.mycat.hbt4.ResponseExecutorImplementor;
import io.mycat.sqlhandler.dml.DrdsRunners;
import io.mycat.util.Response;

public class ShardingSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    @Override
    protected void onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        DrdsRunners.runOnDrds(dataContext,  request.getAst(), ResponseExecutorImplementor.create(dataContext,response));
    }
}
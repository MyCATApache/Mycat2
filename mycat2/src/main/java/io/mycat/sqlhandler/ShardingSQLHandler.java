package io.mycat.sqlhandler;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.MycatDataContext;
import io.mycat.hbt4.DatasourceFactory;
import io.mycat.hbt4.DefaultDatasourceFactory;
import io.mycat.hbt4.ResponseExecutorImplementor;
import io.mycat.sqlhandler.dml.DrdsRunners;
import io.mycat.util.Response;

public class ShardingSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    @Override
    protected void onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        try (DatasourceFactory datasourceFactory = new DefaultDatasourceFactory(dataContext);
             ResponseExecutorImplementor responseExecutorImplementor = ResponseExecutorImplementor.create(dataContext, response, datasourceFactory);) {
            DrdsRunners.runOnDrds(dataContext, request.getAst(), responseExecutorImplementor);
        }
    }
}
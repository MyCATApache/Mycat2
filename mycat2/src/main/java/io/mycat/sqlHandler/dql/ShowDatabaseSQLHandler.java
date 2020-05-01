package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.ast.statement.SQLShowDatabasesStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class ShowDatabaseSQLHandler extends AbstractSQLHandler<SQLShowDatabasesStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLShowDatabasesStatement> request, MycatDataContext dataContext, Response response) {
        response.proxyShow(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

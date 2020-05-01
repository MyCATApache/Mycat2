package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.ast.statement.SQLShowTablesStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class ShowTablesSQLHandler extends AbstractSQLHandler<SQLShowTablesStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLShowTablesStatement> request, MycatDataContext dataContext, Response response) {
        response.proxyShow(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

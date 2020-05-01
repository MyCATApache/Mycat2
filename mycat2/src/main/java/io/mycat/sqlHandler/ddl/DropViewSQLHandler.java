package io.mycat.sqlHandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLDropViewStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class DropViewSQLHandler extends AbstractSQLHandler<SQLDropViewStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLDropViewStatement> request, MycatDataContext dataContext, Response response) {
        response.proxyDDL(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

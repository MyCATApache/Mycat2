package io.mycat.sqlHandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLCreateTableStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class CreateTableSQLHandler extends AbstractSQLHandler<SQLCreateTableStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLCreateTableStatement> request, MycatDataContext dataContext, Response response) {
        response.proxyDDL(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

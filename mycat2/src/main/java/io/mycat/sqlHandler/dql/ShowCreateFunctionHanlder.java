package io.mycat.sqlHandler.dql;

import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class ShowCreateFunctionHanlder  extends AbstractSQLHandler<com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement> request, MycatDataContext dataContext, Response response) {
        response.proxyShow(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}

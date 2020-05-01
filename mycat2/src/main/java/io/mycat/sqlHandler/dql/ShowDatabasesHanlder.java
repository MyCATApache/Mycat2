package io.mycat.sqlHandler.dql;

import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class ShowDatabasesHanlder extends AbstractSQLHandler<com.alibaba.fastsql.sql.ast.statement.SQLShowDatabasesStatement> {
    @Override
    protected ExecuteCode onExecute(SQLRequest<com.alibaba.fastsql.sql.ast.statement.SQLShowDatabasesStatement> request, MycatDataContext dataContext, Response response) {
        response.evalSimpleSql(request.getAst());
        return ExecuteCode.PERFORMED;
    }
}
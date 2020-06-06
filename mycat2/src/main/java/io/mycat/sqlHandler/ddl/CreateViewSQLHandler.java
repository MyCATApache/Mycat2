package io.mycat.sqlHandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLCreateViewStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class CreateViewSQLHandler extends AbstractSQLHandler<SQLCreateViewStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLCreateViewStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}

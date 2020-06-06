package io.mycat.sqlHandler.ddl;

import com.alibaba.fastsql.sql.ast.statement.SQLDropDatabaseStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;

@Resource
public class DropDatabaseSQLHandler extends AbstractSQLHandler<SQLDropDatabaseStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLDropDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        response.sendOk();
        return ExecuteCode.PERFORMED;
    }
}

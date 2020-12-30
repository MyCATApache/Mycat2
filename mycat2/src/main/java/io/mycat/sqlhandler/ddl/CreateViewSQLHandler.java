package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;




public class CreateViewSQLHandler extends AbstractSQLHandler<SQLCreateViewStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLCreateViewStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        response.sendOk();
    }
}

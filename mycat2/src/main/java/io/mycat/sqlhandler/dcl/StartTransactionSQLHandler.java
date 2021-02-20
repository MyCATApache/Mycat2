package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.ast.statement.SQLStartTransactionStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class StartTransactionSQLHandler extends AbstractSQLHandler<SQLStartTransactionStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLStartTransactionStatement> request, MycatDataContext dataContext, Response response){
        return response.begin();
    }
}

package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.impl.future.PromiseInternal;


public class KillSQLHandler extends AbstractSQLHandler<MySqlKillStatement> {

    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<MySqlKillStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        return response.sendOk();
    }
}

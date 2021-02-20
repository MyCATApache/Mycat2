package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.statement.SQLAlterDatabaseStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


public class AlterDatabaseSQLHandler extends AbstractSQLHandler<SQLAlterDatabaseStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLAlterDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        return response.sendOk();
    }
}

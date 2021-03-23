package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.statement.SQLAlterDatabaseStatement;
import io.mycat.LockService;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;


public class AlterDatabaseSQLHandler extends AbstractSQLHandler<SQLAlterDatabaseStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLAlterDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        Future<Lock> lockFuture = lockService.getLockWithTimeout(DDL_LOCK);
        return lockFuture.flatMap(lock -> {
            lock.release();
            return response.sendOk();
        });
    }
}

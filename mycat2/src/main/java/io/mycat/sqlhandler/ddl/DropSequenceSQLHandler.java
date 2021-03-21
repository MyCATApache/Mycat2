package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement;
import io.mycat.*;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

import java.util.function.Function;


public class DropSequenceSQLHandler extends AbstractSQLHandler<com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLDropSequenceStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        Future<Lock> lockFuture = lockService.getLockWithTimeout(getClass().getName());
        return lockFuture.flatMap(lock -> {
            try {
                SQLDropSequenceStatement ast = request.getAst();
                SQLName name = ast.getName();
                if (name instanceof SQLIdentifierExpr) {
                    SQLPropertyExpr sqlPropertyExpr = new SQLPropertyExpr();
                    sqlPropertyExpr.setOwner(dataContext.getDefaultSchema());
                    sqlPropertyExpr.setName(name.toString());
                    ast.setName(sqlPropertyExpr);
                }
                MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                return response.proxyUpdate(metadataManager.getPrototype(), ast.toString());
            }catch (Throwable throwable){
                return Future.failedFuture(throwable);
            }finally {
                lock.release();
            }
        });

    }
}

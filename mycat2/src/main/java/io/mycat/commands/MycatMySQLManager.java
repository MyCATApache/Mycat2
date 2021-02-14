package io.mycat.commands;

import cn.mycat.vertx.xa.MySQLManager;
import io.mycat.*;
import io.mycat.proxy.MySQLDatasourcePool;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.util.VertxUtil;
import io.mycat.vertxmycat.AbstractMySqlConnectionImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MycatMySQLManager implements MySQLManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatMySQLManager.class);
    public MycatMySQLManager() {

    }
    @Override
    public Future<SqlConnection> getConnection(String targetName) {
        PromiseInternal<SqlConnection> promise = VertxUtil.newPromise();
        NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
        MySQLDatasourcePool sqlDatasourcePool = nativeMycatServer.getDatasource(targetName);
        sqlDatasourcePool.createSession().onComplete(event -> {
            MycatWorkerProcessor workerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
            NameableExecutor mycatWorker = workerProcessor.getMycatWorker();
            if (event.failed()){
                mycatWorker.execute(()->promise.tryFail(event.cause()));
            }else {
                mycatWorker.execute(()-> promise.tryComplete(new AbstractMySqlConnectionImpl(event.result())));
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> close() {
        return Future.succeededFuture();
    }

    @Override
    public void setTimer(long delay, Runnable handler) {
        ScheduleUtil.getTimer().schedule(() -> {
             handler.run();
             return null;
        }, delay, TimeUnit.MILLISECONDS);
    }
}

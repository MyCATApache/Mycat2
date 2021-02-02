package io.mycat.commands;

import cn.mycat.vertx.xa.MySQLManager;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatServer;
import io.mycat.ScheduleUtil;
import io.mycat.api.MySQLAPI;
import io.mycat.ext.MySQLAPIImpl;
import io.mycat.proxy.NativeMycatServer;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.util.VertxUtil;
import io.mycat.vertxmycat.AbstractMySqlConnectionImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;

import java.util.concurrent.TimeUnit;

public class MycatMySQLManager implements MySQLManager {

    public MycatMySQLManager() {

    }

    @Override
    public Future<SqlConnection> getConnection(String targetName) {
        PromiseInternal<SqlConnection> promise = VertxUtil.newPromise();
        Thread thread = Thread.currentThread();
        if (!(thread instanceof ReactorEnvThread)){
            MycatServer mycatServer = MetaClusterCurrent.wrapper(MycatServer.class);
            NativeMycatServer mycatServer1 = (NativeMycatServer) mycatServer;
            mycatServer1.getReactorManager().getRandomReactor().addNIOJob(new NIOJob() {
                @Override
                public void run(ReactorEnvThread reactor) throws Exception {
                    MySQLTaskUtil.getMySQLSessionForTryConnect(targetName, new SessionCallBack<MySQLClientSession>() {
                        @Override
                        public void onSession(MySQLClientSession session, Object sender, Object attr) {
                            AbstractMySqlConnectionImpl abstractMySqlConnection = new AbstractMySqlConnectionImpl(session);
                            promise.tryComplete(abstractMySqlConnection);
                        }

                        @Override
                        public void onException(Exception exception, Object sender, Object attr) {
                            promise.fail(exception);
                        }
                    });
                }

                @Override
                public void stop(ReactorEnvThread reactor, Exception reason) {
                    promise.fail(reason);
                }

                @Override
                public String message() {
                    return " public Future<SqlConnection> getConnection(String targetName) ";
                }
            });
        }

        return promise.future();
    }

    @Override
    public void close(Handler<Future> handler) {

    }

    @Override
    public void setTimer(long delay, Runnable handler) {
        ScheduleUtil.getTimer().schedule(() -> {
             handler.run();
             return null;
        }, delay, TimeUnit.MILLISECONDS);
    }
}

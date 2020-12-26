package io.mycat.proxy.session;

import io.mycat.MycatDataContext;
import io.mycat.ThreadUsageEnum;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.bindthread.BindThread;
import io.mycat.bindthread.BindThreadCallback;
import io.mycat.bindthread.BindThreadKey;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * @MySQLProxyServerSession
 * @MySQLServerSession
 */
public class ServerTransactionSessionRunner  {
    final Map<TransactionType, Function<MycatDataContext, TransactionSession>> map;
    private final MycatContextThreadPool threadPool;

    public ServerTransactionSessionRunner(Map<TransactionType, Function<MycatDataContext, TransactionSession>> map,
                                          MycatContextThreadPool threadPool) {
        this.map = map;
        this.threadPool = threadPool;
    }

    public void run(MycatDataContext container, BindThreadCallback runner) {
        TransactionSession transactionSession = container.getTransactionSession();
        if (transactionSession == null) {
            TransactionType transactionType = container.transactionType();
            Objects.requireNonNull(transactionType);
            container.setTransactionSession(transactionSession = map.get(transactionType).apply(container));
        } else {
            if (!transactionSession.name().equals(container.transactionType().getName())) {
                if (transactionSession.isInTransaction()) {
                    throw new IllegalArgumentException("正在处于事务状态,不能切换事务模式");
                } else {
                    //
                    Function<MycatDataContext, TransactionSession> transactionSessionFunction =
                            Objects.requireNonNull(map.get(container.transactionType()));
                    TransactionSession newTransactionSession = transactionSessionFunction.apply(container);

                    newTransactionSession.setReadOnly(transactionSession.isReadOnly());
                    newTransactionSession.setAutocommit(transactionSession.isAutocommit());
                    newTransactionSession.setTransactionIsolation(transactionSession.getTransactionIsolation());

                    container.setTransactionSession(newTransactionSession);
                    transactionSession = newTransactionSession;
                }
            }
        }
        ThreadUsageEnum threadUsageEnum = transactionSession.getThreadUsageEnum();
        run(container, runner, threadUsageEnum);
        return;
    }

    private void run(MycatDataContext container, BindThreadCallback runner, ThreadUsageEnum threadUsageEnum) {
        switch (threadUsageEnum) {
            case THIS_THREADING:
                try {
                    runner.accept(container, null);
                    runner.finallyAccept(container, null);
                } catch (Exception e) {
                    runner.onException(container, e);
                }
                return;
            case MULTI_THREADING:
                threadPool.run(container, runner);
                return;
        }
        throw new IllegalArgumentException();
    }



    public void run(MycatSession session, Runnable runnable) {
        run(session.getDataContext(), getRunner(session, runnable));
    }

   public interface Runnable{
        void run()throws Exception;
    }

    @NotNull
    private BindThreadCallback getRunner(MycatSession session, Runnable runnable) {
        return new BindThreadCallback() {
            @Override
            public void accept(BindThreadKey key, BindThread context) throws Exception{
                runnable.run();
            }

            @Override
            public void finallyAccept(BindThreadKey key, BindThread context) {
                session.runDelayedNioJob();
            }

            @Override
            public void onException(BindThreadKey key, Exception e) {
                session.setLastMessage(e);
                session.writeErrorEndPacketBySyncInProcessError();
            }
        };
    }

//    public void block(MycatDataContext mycatDataContext, Runnable runnable) {
//        if(Thread.currentThread() instanceof ReactorEnvThread){
//          run(mycatDataContext,getRunner(mycatDataContext,runnable),ThreadUsageEnum.MULTI_THREADING);
//            return;
//        }else {
//            run(mycatDataContext,runnable);
//        }
//    }


}
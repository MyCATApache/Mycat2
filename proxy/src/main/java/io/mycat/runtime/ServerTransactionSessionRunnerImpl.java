package io.mycat.runtime;

import io.mycat.MycatDataContext;
import io.mycat.SessionOpt;
import io.mycat.TransactionSession;
import io.mycat.TransactionSessionRunner;
import io.mycat.bindThread.BindThread;
import io.mycat.bindThread.BindThreadCallback;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.proxy.handler.MycatSessionWriteHandler;
import io.mycat.proxy.session.MycatSession;
import io.mycat.thread.GThreadPool;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class ServerTransactionSessionRunnerImpl implements TransactionSessionRunner {
    final Map<String, Function<MycatDataContext, TransactionSession>> map;
    final MycatSession session;
    private final GThreadPool threadPool;

    public ServerTransactionSessionRunnerImpl(Map<String, Function<MycatDataContext, TransactionSession>> map, GThreadPool threadPool,MycatSession session) {
        this.map = map;
        this.threadPool = threadPool;
        this.session = session;
    }

    public void run(BindThreadCallback runner) {
        final AtomicBoolean cancelFlag = new AtomicBoolean(false);
        threadPool.run(new SessionOpt() {
            TransactionSession transactionSession;

            @Override
            public String transactionType() {
                return TransactionSession.LOCAL;
            }

            @Override
            public TransactionSession getTransactionSession() {
                return this.transactionSession;
            }

            @Override
            public void setTransactionSession(TransactionSession session) {
                this.transactionSession = session;
            }

            @Override
            public boolean isRunning() {
                return cancelFlag.get();
            }

            @Override
            public boolean continueBind() {
                return false;
            }
        }, new BindThreadCallback() {
            @Override
            public void accept(BindThreadKey key, BindThread context) {
                runner.accept(key, context);
            }

            @Override
            public void finallyAccept(BindThreadKey key, BindThread context) {
                try {
                    runner.finallyAccept(key, context);
                } finally {
                    cancelFlag.set(true);
                }
            }

            @Override
            public void onException(BindThreadKey key, Exception e) {
                try {
                    runner.onException(key, e);
                } finally {
                    cancelFlag.set(true);
                }
            }
        });
    }

    public void run(MycatDataContext container, BindThreadCallback runner) {
        TransactionSession transactionSession = container.getTransactionSession();
        if (transactionSession == null) {
            String transactionType = container.transactionType();
            Objects.requireNonNull(transactionType);
            container.setTransactionSession(transactionSession = map.get(transactionType).apply(container));
        } else {
            if (!transactionSession.name().equals(container.transactionType())) {
                if (transactionSession.isInTransaction()) {
                    throw new IllegalArgumentException("正在处于事务状态,不能切换事务模式");
                } else {
                    //
                    TransactionSession newTransactionSession = map.get(container.transactionType()).apply(container);

                    newTransactionSession.setReadOnly(transactionSession.isReadOnly());
                    newTransactionSession.setAutocommit(transactionSession.isAutocommit());
                    newTransactionSession.setTransactionIsolation(transactionSession.getTransactionIsolation());

                    container.setTransactionSession(newTransactionSession);
                }
            }
        }
        switch (transactionSession.getThreadUsageEnum()) {
            case THIS_THREADING:
                try {
                    runner.accept(container, null);
                    runner.finallyAccept(container, null);
                } catch (Exception e) {
                    runner.onException(container, e);
                }
                return;
            case BINDING_THREADING:
                threadPool.runOnBinding(container, runner);
                return;
            case MULTI_THREADING:
                threadPool.run(container, runner);
                return;
        }
        throw new IllegalArgumentException();
    }


    @Override
    public void run(MycatDataContext mycatDataContext, Runnable runnable) {
        run(mycatDataContext, new BindThreadCallback() {
            @Override
            public void accept(BindThreadKey key, BindThread context) {
                runnable.run();
            }

            @Override
            public void finallyAccept(BindThreadKey key, BindThread context) {
                MycatSessionWriteHandler writeHandler = session.getWriteHandler();
                switch (writeHandler.getType()) {
                    case SERVER:
                        writeHandler.onLastPacket(session);
                        break;
                    case PROXY:
                        break;
                }
            }

            @Override
            public void onException(BindThreadKey key, Exception e) {
                session.setLastMessage(e);
                session.writeErrorEndPacketBySyncInProcessError();
            }
        });
    }
}
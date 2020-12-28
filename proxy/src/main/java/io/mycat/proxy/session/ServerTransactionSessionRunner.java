package io.mycat.proxy.session;

import io.mycat.MycatDataContext;
import io.mycat.ThreadUsageEnum;
import io.mycat.TransactionSession;
import io.mycat.bindthread.BindThread;
import io.mycat.bindthread.BindThreadCallback;
import io.mycat.bindthread.BindThreadKey;
import org.jetbrains.annotations.NotNull;

/**
 * @MySQLProxyServerSession
 * @MySQLServerSession
 */
public class ServerTransactionSessionRunner {
    final TranscationSwitch transcationSwitch;
    final MycatContextThreadPool threadPool;

    public ServerTransactionSessionRunner(TranscationSwitch transcationSwitch,
                                          MycatContextThreadPool threadPool) {
        this.transcationSwitch = transcationSwitch;
        this.threadPool = threadPool;
    }

    public void run(MycatDataContext container, BindThreadCallback runner) {
        TransactionSession transactionSession = transcationSwitch.ensureTranscation(container);
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

    public interface Runnable {
        void run() throws Exception;
    }

    @NotNull
    private BindThreadCallback getRunner(MycatSession session, Runnable runnable) {
        return new BindThreadCallback() {
            @Override
            public void accept(BindThreadKey key, BindThread context) throws Exception {
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
}
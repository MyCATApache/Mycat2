package io.mycat.lib;

import cn.lightfish.pattern.DynamicSQLMatcher;
import cn.lightfish.pattern.InstructionSet;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.TransactionSession;
import io.mycat.datasource.jdbc.datasource.TransactionSessionUtil;
import io.mycat.datasource.jdbc.thread.GProcess;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.MycatSession;

import java.util.function.Consumer;

public class JdbcExport implements InstructionSet {

    final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcExport.class);

    public static Response beginOnJdbc() {
        return JdbcExport.Lib.begin();
    }

    public static Response commitOnJdbc() {
        return JdbcExport.Lib.commit();
    }
    public static Response rollbackOnJdbc() {
        return JdbcExport.Lib.rollback();
    }
    public static Response queryOnJdbcByDataSource(String sql,String dataSource) {
        return (session, matcher) -> block(session, (Consumer<MycatSession>) session1 -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            MycatResultSetResponse response = TransactionSessionUtil
                    .executeQuery(dataSource, sql);
        });
    }
    public static class Lib {
        public static Response begin() {
            return (session, matcher) -> block(session, (Consumer<MycatSession>) session1 -> {
                TransactionSession transactionSession = ((GThread) Thread.currentThread())
                        .getTransactionSession();
                transactionSession.begin();
                session1.writeOkEndPacket();
            });
        }

        public static Response commit() {
            return (session, matcher) -> block(session, (Consumer<MycatSession>) session1 -> {
                TransactionSession transactionSession = ((GThread) Thread.currentThread())
                        .getTransactionSession();
                transactionSession.commit();
                session1.writeOkEndPacket();
            });
        }

        public static Response rollback() {
            return new Response() {
                @Override
                public void apply(MycatSession session, DynamicSQLMatcher matcher) {
                    TransactionSession transactionSession = ((GThread) Thread.currentThread())
                            .getTransactionSession();
                    transactionSession.rollback();
                    session.writeOkEndPacket();
                }
            };
        }
    }

    public static void block(MycatSession mycat, Consumer<MycatSession> consumer) {
        GRuntime.INSTACNE.run(mycat, new GProcess() {
            @Override
            public void accept(BindThreadKey key, TransactionSession session) {
                try {
                    mycat.deliverWorkerThread((SessionThread) Thread.currentThread());
                    consumer.accept(mycat);
                } finally {
                    mycat.backFromWorkerThread();
                }
            }

            @Override
            public void onException(BindThreadKey key, Exception e) {
                LOGGER.error("", e);
                mycat.setLastMessage(e.toString());
                mycat.writeErrorEndPacket();
            }
        });
    }
}
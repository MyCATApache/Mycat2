package io.mycat.lib.impl;

import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.TransactionSession;
import io.mycat.datasource.jdbc.datasource.TransactionSessionUtil;
import io.mycat.datasource.jdbc.thread.GProcess;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.SQLExecuterWriter;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.MycatSession;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class JdbcLib {

    final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcLib.class);

    public static Response responseQueryOnJdbcByDataSource(String dataSource, String... sql) {
        return (session, matcher) -> block(session, (Consumer<MycatSession>) session1 -> {
            MycatResultSetResponse[] res = new MycatResultSetResponse[sql.length];
            int i = 0;
            for (String s : sql) {
                res[i++] = TransactionSessionUtil
                        .executeQuery(dataSource, s);
            }
            SQLExecuterWriter.writeToMycatSession(session1, res);
        });
    }

    public static Supplier<MycatResultSetResponse[]> queryJdbcByDataSource(String dataSource, String... sql){
        return () -> {
            MycatResultSetResponse[] res = new MycatResultSetResponse[sql.length];
            int i = 0;
            for (String s : sql) {
                res[i++] = TransactionSessionUtil
                        .executeQuery(dataSource, s);
            }
            return res;
        };
    }
    public static Response response(Supplier<MycatResultSetResponse[]> response){
        return (session, matcher) -> block(session, (Consumer<MycatSession>) session1 -> {
            SQLExecuterWriter.writeToMycatSession(session1,  response.get());
        });
    }
    public static Response responseUpdateOnJdbcByDataSource(String dataSource, boolean needGeneratedKeys, String... sql) {
        return (session, matcher) -> block(session, (Consumer<MycatSession>) session1 -> {
            MycatUpdateResponse[] res = new MycatUpdateResponse[sql.length];
            int i = 0;
            for (String s : sql) {
                res[i++] = TransactionSessionUtil
                        .executeUpdate(dataSource, s, needGeneratedKeys);
            }
            SQLExecuterWriter.writeToMycatSession(session1, res);
        });
    }

    public static Response setTransactionIsolation(int transactionIsolation) {
        return (session, matcher) -> block(session, (Consumer<MycatSession>) session1 -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            transactionSession.setTransactionIsolation(transactionIsolation);
            session1.writeOkEndPacket();
        });
    }

    public static Response setAutocommit(boolean autocommit) {
        return (session, matcher) -> block(session, (Consumer<MycatSession>) session1 -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            transactionSession.setAutocommit(autocommit);
            session1.writeOkEndPacket();
        });
    }

    public static Response begin() {
        return (session, matcher) -> block(session, (Consumer<MycatSession>) session1 -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            transactionSession.begin();
            session1.setInTranscation(true);
            session1.writeOkEndPacket();
        });
    }

    public static Response commit() {
        return (session, matcher) -> block(session, (Consumer<MycatSession>) session1 -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            transactionSession.commit();
            session1.setInTranscation(false);
            session1.writeOkEndPacket();
        });
    }

    public static Response rollback() {
        return (session, matcher) -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            transactionSession.rollback();
            session.setInTranscation(false);
            session.writeOkEndPacket();
        };
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
                mycat.getIOThread().addNIOJob(new NIOJob() {
                    @Override
                    public void run(ReactorEnvThread reactor) throws Exception {
                        mycat.writeToChannel();
                    }

                    @Override
                    public void stop(ReactorEnvThread reactor, Exception reason) {

                    }

                    @Override
                    public String message() {
                        return "";
                    }
                });
                mycat.getIOThread().getSelector().wakeup();
            }

            @Override
            public void onException(BindThreadKey key, Exception e) {
                LOGGER.error("", e);
                mycat.setLastMessage(e.toString());
                mycat.writeErrorEndPacket();
            }
        });
    }

    public static Response setTransactionIsolation(String text) {
       return setTransactionIsolation(MySQLIsolation.valueOf(text).getJdbcValue());
    }
}
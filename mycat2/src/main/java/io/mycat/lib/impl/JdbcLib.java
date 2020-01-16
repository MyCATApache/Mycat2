package io.mycat.lib.impl;

import io.mycat.SQLExecuterWriter;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.TransactionSession;
import io.mycat.datasource.jdbc.datasource.TransactionSessionUtil;
import io.mycat.datasource.jdbc.thread.GProcess;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.MycatSession;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class JdbcLib {

    final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcLib.class);

    public static Response responseQueryOnJdbcByDataSource(String dataSource, String... sql) {
        Objects.requireNonNull(dataSource);
        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
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
        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
            SQLExecuterWriter.writeToMycatSession(session1,  response.get());
        });
    }
    public static Response responseUpdateOnJdbcByDataSource(String dataSource, boolean needGeneratedKeys, String... sql) {
        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
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
        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            transactionSession.setTransactionIsolation(transactionIsolation);
            session1.writeOkEndPacket();
        });
    }

    public static Response setAutocommit(boolean autocommit) {
        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            transactionSession.setAutocommit(autocommit);
            session1.writeOkEndPacket();
        });
    }

    public static Response begin() {
        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            transactionSession.begin();
            session1.setInTranscation(true);
            session1.writeOkEndPacket();
        });
    }

    public static Response commit() {
        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            transactionSession.commit();
            session1.setInTranscation(false);
            session1.writeOkEndPacket();
        });
    }

    public static Response rollback() {
        return (session) -> {
            TransactionSession transactionSession = ((GThread) Thread.currentThread())
                    .getTransactionSession();
            transactionSession.rollback();
            session.setInTranscation(false);
            session.writeOkEndPacket();
        };
    }

    public static void block(MycatSession mycat, Consumer<MycatSession> consumer) {
        JdbcRuntime.INSTANCE.run(mycat, new GProcess() {
            @Override
            public void accept(BindThreadKey key, TransactionSession session) {
                Exception ex = null;
                try {
                    TransactionSessionUtil.beforeDoAction();
                    mycat.deliverWorkerThread((SessionThread) Thread.currentThread());
                    consumer.accept(mycat);
                }catch (Exception e){
                    ex = e;
                    TransactionSessionUtil.reset();
                    session.reset();
                }finally {
                    TransactionSessionUtil.afterDoAction();
                    mycat.backFromWorkerThread();
                }
                if (ex==null) {
                    mycat.getIOThread().addNIOJob(new NIOJob() {
                        @Override
                        public void run(ReactorEnvThread reactor) throws Exception {
                            mycat.writeToChannel();
                        }

                        @Override
                        public void stop(ReactorEnvThread reactor, Exception reason) {
                            mycat.setLastMessage(reason);
                            mycat.close(false, reason);
                        }

                        @Override
                        public String message() {
                            return "";
                        }
                    });
                }else {
                    Exception finalEx = ex;
                    mycat.getIOThread().addNIOJob(new NIOJob() {
                        @Override
                        public void run(ReactorEnvThread reactor) throws Exception {
                            LOGGER.error("",finalEx);
                            mycat.setLastMessage(finalEx);
                            mycat.writeErrorEndPacketBySyncInProcessError();
                            mycat.close(false,"");
                        }

                        @Override
                        public void stop(ReactorEnvThread reactor, Exception reason) {
                            mycat.close(false,finalEx);
                        }

                        @Override
                        public String message() {
                            return "";
                        }
                    });
                }
                mycat.getIOThread().getSelector().wakeup();
            }

            @Override
            public void onException(BindThreadKey key, Exception e) {
                LOGGER.error("", e);
                mycat.setLastMessage(e.toString());
                mycat.writeErrorEndPacketBySyncInProcessError();
            }
        });
    }

    public static Response setTransactionIsolation(String text) {
       return setTransactionIsolation(MySQLIsolation.valueOf(text.toUpperCase()).getJdbcValue());
    }
}
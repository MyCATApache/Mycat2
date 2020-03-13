package io.mycat.lib.impl;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.session.MycatSession;

import java.util.function.Consumer;

public class JdbcLib {

    final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcLib.class);
//
//    public static Response responseQueryOnJdbcByDataSource(String dataSource, String... sql) {
//        Objects.requireNonNull(dataSource);
//        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
//            MycatResultSetResponse[] res = new MycatResultSetResponse[sql.length];
//            int i = 0;
//            for (String s : sql) {
//                res[i++] = TransactionSessionUtil
//                        .executeQuery(dataSource, s);
//            }
//            SQLExecuterWriter.writeToMycatSession(session1, res);
//        });
//    }
//
//    public static Supplier<MycatResultSetResponse[]> queryJdbcByDataSource(String dataSource, String... sql) {
//        return () -> {
//            MycatResultSetResponse[] res = new MycatResultSetResponse[sql.length];
//            int i = 0;
//            for (String s : sql) {
//                res[i++] = TransactionSessionUtil
//                        .executeQuery(dataSource, s);
//            }
//            return res;
//        };
//    }
//
//    public static Response response(Supplier<MycatResultSetResponse[]> response) {
//        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
//            SQLExecuterWriter.writeToMycatSession(session1, response.get());
//        });
//    }
//
//    public static Response responseUpdateOnJdbcByDataSource(String dataSource, boolean needGeneratedKeys, String... sql) {
//        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
//            MycatUpdateResponse[] res = new MycatUpdateResponse[sql.length];
//            int i = 0;
//            for (String s : sql) {
//                res[i++] = TransactionSessionUtil
//                        .executeUpdate(dataSource, s, needGeneratedKeys);
//            }
//            SQLExecuterWriter.writeToMycatSession(session1, res);
//        });
//    }
//
//    public static Response setTransactionIsolation(int transactionIsolation) {
//        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
//            TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
//            ;
//            transactionSession.setTransactionIsolation(transactionIsolation);
//            session1.writeOkEndPacket();
//        });
//    }
//
//    public static Response setAutocommit(boolean autocommit) {
//        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
//            TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
//            ;
//            transactionSession.setAutocommit(autocommit);
//            session1.writeOkEndPacket();
//        });
//    }
//
//    public static Response begin() {
//        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
//            TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
//            ;
//            transactionSession.begin();
//            session1.setInTranscation(true);
//            session1.writeOkEndPacket();
//        });
//    }
//
//    public static Response commit() {
//        return (session) -> block(session, (Consumer<MycatSession>) session1 -> {
//            TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
//            ;
//            transactionSession.commit();
//            session1.setInTranscation(false);
//            session1.writeOkEndPacket();
//        });
//    }
//
//    public static Response rollback() {
//        return (session) -> {
//            TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
//            ;
//            transactionSession.rollback();
//            session.setInTranscation(false);
//            session.writeOkEndPacket();
//        };
//    }

    public static void block(MycatSession mycat, Consumer<MycatSession> consumer) {
        consumer.accept(mycat);
//        JdbcRuntime.INSTANCE.run(mycat, new GProcess() {
//            @Override
//            public void accept(BindThreadKey key) {
//
//            }
//
//            Exception ex = null;
//
//
//
//            @Override
//            public void accept(BindThreadKey key, BindThread context) {
//                try {
//                    mycat.deliverWorkerThread((SessionThread) Thread.currentThread());
//                    consumer.accept(mycat);
//                } catch (Exception e) {
//                    ex = e;
//                }
//            }
//
//            @Override
//            public void finallyAccept(BindThreadKey key, BindThread context) {
//                mycat.backFromWorkerThread();
//                if (ex == null) {
//                    mycat.getIOThread().addNIOJob(new NIOJob() {
//                        @Override
//                        public void run(ReactorEnvThread reactor) throws Exception {
//                            if (mycat.writeQueue().size() != 2) {
//                                throw new AssertionError();
//                            }
//                            mycat.switchMySQLServerWriteHandler();
//                            mycat.writeToChannel();
//                        }
//
//                        @Override
//                        public void stop(ReactorEnvThread reactor, Exception reason) {
//                            mycat.setLastMessage(reason);
//                            mycat.close(false, reason);
//                        }
//
//                        @Override
//                        public String message() {
//                            return "";
//                        }
//                    });
//                } else {
//                    Exception finalEx = ex;
//                    mycat.getIOThread().addNIOJob(new NIOJob() {
//                        @Override
//                        public void run(ReactorEnvThread reactor) throws Exception {
//                            LOGGER.error("", finalEx);
//                            mycat.setLastMessage(finalEx);
//                            mycat.writeErrorEndPacketBySyncInProcessError();
//                            mycat.close(false, "");
//                        }
//
//                        @Override
//                        public void stop(ReactorEnvThread reactor, Exception reason) {
//                            mycat.close(false, finalEx);
//                        }
//
//                        @Override
//                        public String message() {
//                            return "";
//                        }
//                    });
//                }
//                mycat.getIOThread().getSelector().wakeup();
//            }
//
//            @Override
//            public void onException(BindThreadKey key, Exception e) {
//                LOGGER.error("", e);
//                mycat.setLastMessage(e.toString());
//                mycat.writeErrorEndPacketBySyncInProcessError();
//            }
//        });
    }
//
//    public static Response setTransactionIsolation(String text) {
//        return setTransactionIsolation(MySQLIsolation.valueOf(text.toUpperCase()).getJdbcValue());
//    }
}
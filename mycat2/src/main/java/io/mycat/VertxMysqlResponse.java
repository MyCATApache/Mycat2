//package io.mycat;
//
//import io.mycat.util.packet.BeginWritePacket;
//import io.mycat.util.packet.CommitWritePacket;
//import io.mycat.util.packet.RollbackWritePacket;
//import io.mycat.vertx.VertxResponse;
//import io.mycat.vertx.VertxSession;
//import io.vertx.core.impl.future.PromiseInternal;
//
//public class VertxMysqlResponse extends VertxResponse {
//    public VertxMysqlResponse(VertxSession session, int size, boolean binary) {
//        super(session, size, binary);
//    }
//
//
//    @Override
//    public PromiseInternal<Void> rollback() {
//        dataContext.getEmitter().onNext(new RollbackWritePacket(){
//            @Override
//            public void writeToSocket() {
//                count++;
//                TransactionSession transactionSession = dataContext.getTransactionSession();
//                transactionSession.rollback();
//                transactionSession.closeStatenmentState();
//                session.writeOk(count<size);
//            }
//        });
//    }
//
//    @Override
//    public PromiseInternal<Void>  begin() {
//        dataContext.getEmitter().onNext(new BeginWritePacket(){
//            @Override
//            public void writeToSocket() {
//                count++;
//                TransactionSession transactionSession = dataContext.getTransactionSession();
//                transactionSession.begin();
//                session.writeOk(count<size);
//            }
//        });
//    }
//
//    @Override
//    public PromiseInternal<Void>  commit() {
//        dataContext.getEmitter().onNext(new CommitWritePacket(){
//            @Override
//            public void writeToSocket() {
//                count++;
//                TransactionSession transactionSession = dataContext.getTransactionSession();
//                transactionSession.commit();
//                transactionSession.closeStatenmentState();
//                session.writeOk(count<size);
//            }
//        });
//    }
//
//    @Override
//    public PromiseInternal<Void>  execute(ExplainDetail detail) {
//
//    }
//
//
//}

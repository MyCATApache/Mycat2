package io.mycat.vertx;

import io.mycat.TransactionSession;
import io.mycat.util.packet.BeginWritePacket;
import io.mycat.util.packet.CommitWritePacket;
import io.mycat.util.packet.RollbackWritePacket;

public class VertxJdbcResponseImpl extends VertxResponse {
    public VertxJdbcResponseImpl(VertxSession session, int size, boolean binary) {
        super(session, size, binary);
    }


    @Override
    public void rollback() {
        dataContext.getEmitter().onNext(new RollbackWritePacket(){
            @Override
            public void writeToSocket() {
                count++;
                TransactionSession transactionSession = dataContext.getTransactionSession();
                transactionSession.rollback();
                transactionSession.closeStatenmentState();
                session.writeOk(count<size);
            }
        });
    }

    @Override
    public void begin() {
        dataContext.getEmitter().onNext(new BeginWritePacket(){
            @Override
            public void writeToSocket() {
                count++;
                TransactionSession transactionSession = dataContext.getTransactionSession();
                transactionSession.begin();
                session.writeOk(count<size);
            }
        });
    }

    @Override
    public void commit() {
        dataContext.getEmitter().onNext(new CommitWritePacket(){
            @Override
            public void writeToSocket() {
                count++;
                TransactionSession transactionSession = dataContext.getTransactionSession();
                transactionSession.commit();
                transactionSession.closeStatenmentState();
                session.writeOk(count<size);
            }
        });
    }


}

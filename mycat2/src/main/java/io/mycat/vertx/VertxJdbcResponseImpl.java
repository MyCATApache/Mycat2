package io.mycat.vertx;

import io.mycat.MySQLPacketUtil;
import io.mycat.ResultSetMapping;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.resultset.BinaryResultSetResponse;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;

import java.util.Iterator;
import java.util.function.Function;

public class VertxJdbcResponseImpl extends VertxResponse {


    public VertxJdbcResponseImpl(VertxSession session, int size, boolean binary) {
        super(session, size, binary);
    }


    @Override
    public void sendResultSet(Observable<Object[]> observable, MycatRowMetaData mycatRowMetaData, ResultType resultType) {
        count++;
        boolean moreResultSet = count < size;
        session.writeColumnCount(mycatRowMetaData.getColumnCount());
        Iterator<byte[]> columnIterator = MySQLPacketUtil.generateAllColumnDefPayload(mycatRowMetaData).iterator();
        while (columnIterator.hasNext()) {
            session.writeBytes(columnIterator.next(), false);
        }
        session.writeColumnEndPacket();
        Function<Object[], byte[]> convert = ResultSetMapping.concertToDirectResultSet(mycatRowMetaData);
        observable.map(objects -> convert.apply(objects)).subscribe(new Observer<byte[]>() {
            Disposable disposable;

            @Override
            public void onSubscribe(@NonNull Disposable d) {
                disposable = d;
            }

            @Override
            public void onNext(byte @NonNull [] bytes) {
                session.writeBytes(bytes, false);
            }

            @Override
            public void onError(@NonNull Throwable e) {
                disposable.dispose();
                session.getDataContext().getTransactionSession().closeStatenmentState();
                sendError(e);
            }

            @Override
            public void onComplete() {
                session.getDataContext().getTransactionSession().closeStatenmentState();
                session.writeRowEndPacket(moreResultSet, false);
            }
        });
}

    @Override
    public void rollback() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.rollback();
        transactionSession.closeStatenmentState();
        session.writeOk(count < size);
    }

    @Override
    public void begin() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.begin();
        session.writeOk(count < size);
    }

    @Override
    public void commit() {
        count++;
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.commit();
        transactionSession.closeStatenmentState();
        session.writeOk(count < size);
    }

}

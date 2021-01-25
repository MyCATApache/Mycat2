package io.mycat.vertx;

import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.resultset.BinaryResultSetResponse;
import io.mycat.resultset.DirectTextResultSetResponse;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.util.VertxUtil;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.impl.future.PromiseInternal;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import static io.mycat.ExecuteType.QUERY;
import static io.mycat.ExecuteType.UPDATE;

public abstract class VertxResponse implements Response {

    protected final MycatDataContext dataContext;
    protected VertxSession session;
    protected final int size;
    protected int count;
    protected boolean binary;

    public VertxResponse(VertxSession session, int size, boolean binary) {
        this.session = session;
        this.size = size;
        this.binary = binary;
        this.dataContext = session.getDataContext();
    }

    @Override
    public PromiseInternal<Void> proxySelect(String defaultTargetName, String statement) {
        return execute(ExplainDetail.create(QUERY, defaultTargetName, statement, null));
    }

    @Override
    public PromiseInternal<Void> proxyUpdate(String defaultTargetName, String proxyUpdate) {
        return execute(ExplainDetail.create(UPDATE, Objects.requireNonNull(defaultTargetName), proxyUpdate, null));
    }

    @Override
    public PromiseInternal<Void> sendError(Throwable e) {
        dataContext.getTransactionSession().closeStatenmentState();
        dataContext.setLastMessage(e);
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public PromiseInternal<Void> proxySelectToPrototype(String statement) {
        return proxySelect("prototype", statement);
    }


    @Override
    public PromiseInternal<Void> sendError(String errorMessage, int errorCode) {
        dataContext.getTransactionSession().closeStatenmentState();
        dataContext.setLastMessage(errorMessage);
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public PromiseInternal<Void> sendResultSet(RowIterable rowIterable) {
        // todo 异步未实现完全 wangzihaogithub 这里需要改成 write write flush
        return getPromiseInternal(new IterableTask(rowIterable) {
            @Override
            public void onCloseResource() {
                dataContext.getTransactionSession().closeStatenmentState();
            }
        });
    }

    protected PromiseInternal getPromiseInternal(IterableTask iterableTask) {
        RowBaseIterator resultSet = iterableTask.rowIterable.get();
        ++count;
        boolean moreResultSet = count < size;
        MycatResultSetResponse currentResultSet;
        if (!binary) {
            if (resultSet instanceof JdbcRowBaseIterator) {
                currentResultSet = new DirectTextResultSetResponse((resultSet));
            } else {
                currentResultSet = new TextResultSetResponse(resultSet);
            }
        } else {
            currentResultSet = new BinaryResultSetResponse(resultSet);
        }
        session.writeColumnCount(currentResultSet.columnCount());
        Iterator<byte[]> columnDefPayloadsIterator = currentResultSet
                .columnDefIterator();
        while (columnDefPayloadsIterator.hasNext()) {
            session.writeBytes(columnDefPayloadsIterator.next(), false);
        }
        session.writeColumnEndPacket();
        Iterator<byte[]> rowIterator = currentResultSet.rowIterator();
        while (rowIterator.hasNext()) {
            byte[] row = rowIterator.next();
            session.writeBytes(row, false);
        }
        currentResultSet.close();
        iterableTask.onCloseResource();
        PromiseInternal promiseInternal = session.writeRowEndPacket(moreResultSet, false);
        iterableTask.onCloseResource();
        return promiseInternal;
    }

    @Override
    public PromiseInternal<Void> execute(ExplainDetail detail) {
        PromiseInternal<Void> promise;
        String target = detail.getTarget();
        ExecuteType executeType = detail.getExecuteType();
        String sql = detail.getSql();
        MycatDataContext dataContext = session.getDataContext();

        switch (executeType) {
            case QUERY:
                target = dataContext.resolveDatasourceTargetName(target, false);
                break;
            case QUERY_MASTER:
            case INSERT:
            case UPDATE:
            default:
                target = dataContext.resolveDatasourceTargetName(target, true);
                break;
        }
        TransactionSession transactionSession = dataContext.getTransactionSession();
        MycatConnection connection = transactionSession.getJDBCConnection(target);
        count++;
        switch (executeType) {
            case QUERY:
            case QUERY_MASTER:
                promise = sendResultSet(connection.executeQuery(null, sql));
                break;
            case INSERT:
            case UPDATE:
                long[] longs = connection.executeUpdate(sql, true);
                transactionSession.closeStatenmentState();
                promise = sendOk(longs[0], longs[1]);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + executeType);
        }
        return promise;
    }

    @Override
    public PromiseInternal<Void> sendOk(long affectedRow, long lastInsertId) {
        count++;
        MycatDataContext dataContext = session.getDataContext();
        dataContext.getTransactionSession().closeStatenmentState();
        dataContext.setLastInsertId(lastInsertId);
        dataContext.setAffectedRows(affectedRow);
        return session.writeOk(count < size);
    }

    @Override
    public PromiseInternal<Void> sendOk() {
        count++;
        MycatDataContext dataContext = session.getDataContext();
        dataContext.getTransactionSession().closeStatenmentState();
        return session.writeOk(count < size);
    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        return null;
    }

    @Override
    public PromiseInternal<Void> sendResultSet(RowObservable rowIterable) {
        count++;
        PromiseInternal<Void> promise = VertxUtil.newSuccessPromise();
        rowIterable.subscribe();
        ObserverWrite observerWrite = new ObserverWrite(new ObserverTask(rowIterable) {
            @Override
            public void onCloseResource() {
                dataContext.getTransactionSession().closeStatenmentState();
            }

            @Override
            public void onError(Throwable throwable) {
                promise.fail(throwable);
            }

            @Override
            public void onComplete() {
                promise.complete();
            }
        });
        rowIterable.subscribe(observerWrite);
        return promise;
    }


    public class ObserverWrite implements Observer<Object[]> {
        private final RowObservable rowIterable;
        private ObserverTask observerTask;
        boolean moreResultSet;
        Function<Object[], byte[]> convertor;
        Disposable disposable;

        public ObserverWrite(ObserverTask observerTask) {
            this.rowIterable = observerTask.rowIterable;
            this.observerTask = observerTask;
        }

        @Override
        public void onSubscribe(@NonNull Disposable d) {
            this.disposable = d;
            MycatRowMetaData rowMetaData = rowIterable.getRowMetaData();
            this.moreResultSet = count < size;
            session.writeColumnCount(rowMetaData.getColumnCount());
            if (!binary) {
                this.convertor = ResultSetMapping.concertToDirectTextResultSet(rowMetaData);
            } else {
                this.convertor = ResultSetMapping.concertToDirectBinaryResultSet(rowMetaData);
            }
            Iterator<byte[]> columnIterator = MySQLPacketUtil.generateAllColumnDefPayload(rowMetaData).iterator();
            while (columnIterator.hasNext()) {
                session.writeBytes(columnIterator.next(), false);
            }
            session.writeColumnEndPacket();
        }

        @Override
        public void onNext(Object @NonNull [] objects) {
            session.writeBytes(this.convertor.apply(objects), false);
            ;
        }

        @Override
        public void onError(@NonNull Throwable e) {
            session.getDataContext().getTransactionSession().closeStatenmentState();
            disposable.dispose();
            this.observerTask.onCloseResource();
            this.observerTask.onError(e);
        }

        @Override
        public void onComplete() {
            disposable.dispose();
            this.observerTask.onCloseResource();
            session.writeRowEndPacket(moreResultSet, false);
            this.observerTask.onComplete();
        }

    }

    public static abstract class ObserverTask {
        private final RowObservable rowIterable;

        public ObserverTask(RowObservable rowIterable) {
            this.rowIterable = rowIterable;
        }

        public abstract void onCloseResource();

        public abstract void onError(Throwable throwable);

        public abstract void onComplete();
    }

    public static abstract class IterableTask {
        private final RowIterable rowIterable;

        public IterableTask(RowIterable rowIterable) {
            this.rowIterable = rowIterable;
        }

        public abstract void onCloseResource();
    }
}
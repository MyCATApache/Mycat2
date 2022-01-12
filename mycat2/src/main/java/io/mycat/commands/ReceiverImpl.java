/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.commands;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.*;
import io.mycat.api.collector.*;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.resultset.ResultSetWriter;
import io.mycat.beans.resultset.SimpleBinaryWriterImpl;
import io.mycat.beans.resultset.SimpleTextWriterImpl;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.RowSet;
import io.mycat.newquery.SqlResult;
import io.mycat.proxy.session.MySQLServerSession;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.ResultSetMapping;
import io.mycat.vertx.VertxExecuter;
import io.ordinate.engine.builder.SchemaBuilder;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.util.ResultWriterUtil;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableSource;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.mycat.ExecuteType.*;


public class ReceiverImpl implements Response {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverImpl.class);

    protected final MySQLServerSession session;
    protected final MycatDataContext dataContext;
    protected final XaSqlConnection transactionSession;
    private final int stmtSize;
    private final boolean binary;
    protected int count = 0;

    public ReceiverImpl(MySQLServerSession session, int stmtSize, boolean binary) {
        this.stmtSize = stmtSize;
        this.binary = binary;
        this.session = session;
        this.dataContext = this.session.getDataContext();
        this.transactionSession = (XaSqlConnection) this.dataContext.getTransactionSession();
    }

    @Override
    public int getResultSetCounter() {
        return this.count;
    }

    @Override
    public void resetResultSetCounter(int count) {
        this.count = count;
    }

    @Override
    public Future<Void> sendError(Throwable e) {
        MycatDataContext dataContext = session.getDataContext();
        dataContext.setLastMessage(e);
        return VertxUtil.newFailPromise(new RuntimeException(e));
    }

    @Override
    public Future<Void> proxySelect(List<String> targets, String statement,List<Object> params) {
        return execute(ExplainDetail.create(QUERY, targets, statement, null,params));
    }

    @Override
    public Future<Void> proxyInsert(List<String> targets, String proxyUpdate,List<Object> params) {
        return execute(ExplainDetail.create(INSERT, targets, proxyUpdate, null,params));
    }


    @Override
    public Future<Void> proxyUpdate(List<String> targets, String sql,List<Object> params) {
        return execute(ExplainDetail.create(UPDATE, Objects.requireNonNull(targets), sql, null,params));
    }

    @Override
    public Future<Void> proxyUpdateToPrototype(String proxyUpdate,List<Object> params) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        return proxyUpdate(Collections.singletonList(Objects.requireNonNull(metadataManager.getPrototype())), proxyUpdate,params);
    }

    @Override
    public Future<Void> proxySelectToPrototype(String statement,List<Object> params) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        return execute(ExplainDetail.create(QUERY_MASTER, Collections.singletonList(Objects.requireNonNull(metadataManager.getPrototype())), statement, null,params));
    }


    @Override
    public Future<Void> sendError(String errorMessage, int errorCode) {
        return VertxUtil.newFailPromise(new MycatException(errorCode, errorMessage));
    }

    @Override
    public Future<Void> sendResultSet(Observable<MysqlPayloadObject> mysqlPacketObservable) {
        count++;
        boolean hasMoreResult = hasMoreResultSet();
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);

        return ioExecutor.executeBlocking(promise1 -> mysqlPacketObservable.subscribe(
                new MysqlPayloadObjectObserver(promise1, hasMoreResult, binary, session)));

    }

    @Override
    public Future<Void> rollback() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        return transactionSession.rollback()
                .eventually((u) -> transactionSession.closeStatementState())
                .flatMap(u -> session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> begin() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        return transactionSession.begin()
                .eventually((u) -> transactionSession.closeStatementState())
                .flatMap(u -> session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> commit() {
        count++;
        boolean moreResultSet = hasMoreResultSet();
        return transactionSession.commit().eventually((u) -> transactionSession.closeStatementState())
                .flatMap(u -> session.writeOk(moreResultSet));
    }

    @Override
    public Future<Void> rollbackSavepoint(String name) {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        return transactionSession.rollbackSavepoint(name)
                .eventually((u) -> transactionSession.closeStatementState())
                .flatMap(u -> session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> setSavepoint(String name) {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        return transactionSession.createSavepoint(name)
                .eventually((u) -> transactionSession.closeStatementState())
                .flatMap(u -> session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> releaseSavepoint(String name) {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        return transactionSession.releaseSavepoint(name)
                .eventually((u) -> transactionSession.closeStatementState())
                .flatMap(u -> session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> execute(ExplainDetail detail) {
        boolean directPacket = false;
        boolean master = dataContext.isInTransaction() || !dataContext.isAutocommit() || detail.getExecuteType().isMaster();
        Set<String> targets = new HashSet<>();
        for (String target : detail.getTargets()) {
            String datasource = session.getDataContext().resolveDatasourceTargetName(target, master);
            targets.add(datasource);
        }

        String sql = detail.getSql();
        ArrayList<String> targetOrderList = new ArrayList<>(targets);
        switch (detail.getExecuteType()) {
            case QUERY:
            case QUERY_MASTER: {
                List<Observable<MysqlPayloadObject>> outputs = new LinkedList<>();
                for (int i = 0; i < targetOrderList.size(); i++) {
                    String datasource = targetOrderList.get(i);
                    Future<NewMycatConnection> connectionFuture = transactionSession.getConnection(datasource);
                    if (i == 0) {
                        outputs.add(VertxExecuter.runQueryOutputAsMysqlPayloadObject(connectionFuture, sql.toString(), detail.getParams()));
                    } else {
                        outputs.add(VertxExecuter.runQuery(connectionFuture, sql.toString(), Collections.emptyList(), null)
                                .map(row -> new MysqlObjectArrayRow(row)));
                    }
                }
                return sendResultSet(Observable.concat(outputs));
            }
            case UPDATE:
            case INSERT:
                List<Future<long[]>> updateInfoList = new ArrayList<>(targetOrderList.size());
                for (int i = 0; i < targetOrderList.size(); i++) {
                    String datasource = targetOrderList.get(i);
                    Future<NewMycatConnection> connectionFuture = transactionSession.getConnection(datasource);
                    if (detail.getExecuteType() == INSERT) {
                        updateInfoList.add(VertxExecuter.runInsert(connectionFuture, sql.toString(),detail.getParams()));
                    } else {
                        updateInfoList.add(VertxExecuter.runUpdate(connectionFuture, sql.toString(),detail.getParams()));
                    }
                }
                CompositeFuture all = CompositeFuture.join((List) updateInfoList)
                        .onSuccess(unused -> dataContext.getTransactionSession().closeStatementState());
                return all.map(u -> {
                    List<long[]> list = all.list();
                    return list.stream().reduce(new long[]{0, 0}, (o, o2) -> new long[]{o[0] + o2[0], o[1] + o2[1]});
                }).flatMap(result -> {
                    return sendOk(result[0], result[1]);
                });
            default:
                throw new IllegalStateException("Unexpected value: " + detail.getExecuteType());
        }
    }

    @Override
    public Future<Void> sendOk() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        return transactionSession.closeStatementState().flatMap(u -> session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> sendOk(long affectedRow) {
        dataContext.setAffectedRows(affectedRow);
        return sendOk();
    }

    @Override
    public Future<Void> sendOk(long affectedRow, long lastInsertId) {
        dataContext.setLastInsertId(lastInsertId);
        dataContext.setAffectedRows(affectedRow);
        return sendOk();
    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        return null;
    }

    @Override
    public Future<Void> swapBuffer(Observable<Buffer> sender) {
        return Future.future(new Handler<Promise<Void>>() {
            Future<Void> future = Future.succeededFuture();
            @Override
            public void handle(Promise<Void> promise) {
                session.directWriteStart();
                sender.subscribe(buffer -> future= session.directWrite(buffer),
                        throwable -> promise.tryFail(throwable),
                        () -> future.flatMap((Function<Void, Future<Void>>) unused -> session.directWriteEnd()).onComplete(event -> {
                            if (event.succeeded()){
                                promise.tryComplete();
                            }else {
                                promise.tryFail(event.cause());
                            }
                        }));
            }
        });
    }


    protected boolean hasMoreResultSet() {
        return count < this.stmtSize;
    }


    public static class MysqlPayloadObjectObserver implements Observer<MysqlPayloadObject> {
        private final Promise<Void> promise;
        private final boolean moreResultSet;
        private boolean binary;
        private MySQLServerSession session;
        private Disposable disposable;
        Function<Object[], byte[]> convertor;
        private final AtomicLong rowCount = new AtomicLong(0);

        public MysqlPayloadObjectObserver(Promise<Void> promise,
                                          boolean moreResultSet, boolean binary, MySQLServerSession session) {
            this.promise = promise;
            this.moreResultSet = moreResultSet;
            this.binary = binary;
            this.session = session;
        }

        @Override
        public void onSubscribe(@NonNull Disposable d) {
            this.disposable = d;
        }

        @Override
        public void onNext(@NonNull MysqlPayloadObject next) {
            if (next instanceof MysqlObjectArrayRow) {
                rowCount.getAndIncrement();
                session.writeBytes(this.convertor.apply(((MysqlObjectArrayRow) next).getRow()), false);
            } else if (next instanceof MysqlByteArrayPayloadRow) {
                rowCount.getAndIncrement();
                session.writeBytes(((MysqlByteArrayPayloadRow) next).getBytes(), false);
            } else if (next instanceof MySQLColumnDef) {
                MycatRowMetaData rowMetaData = ((MySQLColumnDef) next).getMetaData();
                session.writeColumnCount(rowMetaData.getColumnCount());
                if (!binary) {
                    convertor = ResultSetMapping.concertToDirectTextResultSet(rowMetaData);
                } else {
                    convertor = ResultSetMapping.concertToDirectBinaryResultSet(rowMetaData);
                }
                Iterator<byte[]> columnIterator = MySQLPacketUtil.generateAllColumnDefPayload(rowMetaData).iterator();
                while (columnIterator.hasNext()) {
                    session.writeBytes(columnIterator.next(), false);
                }
                session.writeColumnEndPacket(moreResultSet);
            }
        }

        @Override
        public void onError(@NonNull Throwable throwable) {
            disposable.dispose();
            promise.tryFail(throwable);
        }

        @Override
        public void onComplete() {
            // session.getDataContext().setAffectedRows(rowCount.get());
            disposable.dispose();
            session.getDataContext().getTransactionSession().closeStatementState()
                    .onComplete(event -> {
                        session.writeRowEndPacket(moreResultSet, false)
                                .onComplete((Handler<AsyncResult>) event1 -> promise.tryComplete());
                    });
        }
    }

    @Override
    public Future<Void> sendVectorResultSet(MycatRowMetaData mycatRowMetaData,
                                            Observable<VectorSchemaRoot> rootObservable) {
        class Writer implements io.reactivex.rxjava3.functions.Function<VectorSchemaRoot, ObservableSource<? extends MysqlPayloadObject>> {
            InnerType[] types = null;

            @Override
            public ObservableSource<? extends MysqlPayloadObject> apply(VectorSchemaRoot vectorRowBatch) throws Throwable {
                int rowCount = vectorRowBatch.getRowCount();
                ArrayList<MysqlPayloadObject> objects = new ArrayList<>(rowCount);
                if (types == null) {
                    types = SchemaBuilder.getInnerTypes(vectorRowBatch);
                }
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    ResultSetWriter newWriter = binary ? new SimpleBinaryWriterImpl() : new SimpleTextWriterImpl();
                    ResultWriterUtil.vectorRowBatchToResultSetWriter(vectorRowBatch, newWriter, types, rowId);
                    objects.add(MysqlByteArrayPayloadRow.of(newWriter.build()));
                }
                return Observable.fromIterable(objects);
            }
        };
        Writer writer = new Writer();
        Observable<MySQLColumnDef> mySQLColumnDefObservable = Observable.fromArray(MySQLColumnDef.of(mycatRowMetaData));
        Observable<MysqlPayloadObject> mysqlPayloadObjectObservable = rootObservable.flatMap(writer);
        Observable<MysqlPayloadObject> mysqlPacketObservable = Observable.concat(mySQLColumnDefObservable, mysqlPayloadObjectObservable);
        return sendResultSet(mysqlPacketObservable);
    }


    @Override
    public Future<Void> proxyProcedure(String sql, String targetName) {
        targetName = dataContext.resolveDatasourceTargetName(targetName, true);
        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
        Future<NewMycatConnection> mySQLManagerConnection = transactionSession.getConnection(targetName);
        Future<List<Object>> objectFuture = mySQLManagerConnection.flatMap(newMycatConnection -> {
            Future<List<Object>> call = newMycatConnection.call(sql);
            return (Future) call;
        });
        Future<List<Object>> rowBaseIteratorFuture = objectFuture.map(objects -> objects.stream().map(o -> {
            if (o instanceof long[]) return o;
            if (o instanceof SqlResult) return ((SqlResult) o).toLongs();
            if (o instanceof RowSet) return ((RowSet) o).toRowBaseIterator();
            throw new UnsupportedOperationException();
        }).collect(Collectors.toList()));
        return rowBaseIteratorFuture.flatMap(objectList -> {
            if (objectList instanceof List) {
                List list = (List) objectList;
                int thisStmtResultSetSize = list.size();
                int resultSetCounter = getResultSetCounter();
                resetResultSetCounter(resultSetCounter + thisStmtResultSetSize - 1);
                Future<Void> future = Future.succeededFuture();
                for (Object o : list) {
                    if (o instanceof long[]) {
                        long[] r = (long[]) o;
                        future = future.flatMap(unused -> sendOk(r[0], r[1]));
                    } else if (o instanceof RowBaseIterator) {
                        RowBaseIterator rs = (RowBaseIterator) o;
                        future = future.flatMap(unused -> sendResultSet(rs));
                    }
                    return future;
                }
            }
            throw new UnsupportedOperationException();
        });
    }

}
package io.mycat.connection;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.Iterators;
import io.mycat.*;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.MysqlObjectArrayRow;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.swapbuffer.PacketRequest;
import io.mycat.swapbuffer.PacketResponse;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.core.Emitter;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.*;
import lombok.Getter;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.*;

import static io.mycat.ExecuteType.*;

@Getter
public class JdbcResponse implements Response {

    private MycatDataContext dataContext;
    private long affectedRow;
    private long lastInsertId;
    private Iterator<Object[]> iterator;
    private MycatRowMetaData metaData;

    public JdbcResponse(MycatDataContext dataContext) {
        this.dataContext = dataContext;
    }

    @Override
    public int getResultSetCounter() {
        return 0;
    }

    @Override
    public void resetResultSetCounter(int count) {

    }

    @Override
    public Future<Void> sendError(Throwable e) {
        return Future.failedFuture(e);
    }

    @Override
    public Future<Void> proxySelect(List<String> targets, String statement) {
        return execute(ExplainDetail.create(QUERY, Objects.requireNonNull(targets), statement, null));
    }

    @Override
    public Future<Void> proxyInsert(List<String> targets, String proxyUpdate) {
        return execute(ExplainDetail.create(INSERT, Objects.requireNonNull(targets), proxyUpdate, null));
    }

    @Override
    public Future<Void> proxyUpdate(List<String> targets, String proxyUpdate) {
        return execute(ExplainDetail.create(UPDATE, Objects.requireNonNull(targets), proxyUpdate, null));
    }

    @Override
    public Future<Void> proxyUpdateToPrototype(String proxyUpdate) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        return execute(ExplainDetail.create(UPDATE, Collections.singletonList(metadataManager.getPrototype()), proxyUpdate, null));
    }

    @Override
    public Future<Void> proxySelectToPrototype(String statement) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        return execute(ExplainDetail.create(QUERY, Collections.singletonList(metadataManager.getPrototype()), statement, null));
    }

    @Override
    public Future<Void> sendError(String errorMessage, int errorCode) {
        return Future.failedFuture(new MycatException(errorCode, errorMessage));
    }

    @Override
    public Future<Void> rollback() {
        this.affectedRow = 0;
        this.lastInsertId = 0;
        return dataContext.getTransactionSession().rollback();
    }

    @Override
    public Future<Void> begin() {
        this.affectedRow = 0;
        this.lastInsertId = 0;
        return dataContext.getTransactionSession().begin();
    }

    @Override
    public Future<Void> commit() {
        this.affectedRow = 0;
        this.lastInsertId = 0;
        return dataContext.getTransactionSession().commit();
    }

    @Override
    public Future<Void> execute(ExplainDetail detail) {
        this.affectedRow = 0;
        this.lastInsertId = 0;
        boolean directPacket = false;
        boolean master = dataContext.isInTransaction() || !dataContext.isAutocommit() || detail.getExecuteType().isMaster();
        Set<String> targets = new HashSet<>();
        for (String target : detail.getTargets()) {
            String datasource = dataContext.resolveDatasourceTargetName(target, master);
            targets.add(datasource);
        }
        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
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
                        outputs.add(VertxExecuter.runQueryOutputAsMysqlPayloadObject(connectionFuture, sql, detail.getParams()));
                    } else {
                        outputs.add(VertxExecuter.runQuery(connectionFuture, sql, Collections.emptyList(), null)
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
                    if (detail.getExecuteType() == INSERT){
                        updateInfoList.add(VertxExecuter.runInsert(connectionFuture, sql));
                    }else {
                        updateInfoList.add(VertxExecuter.runUpdate(connectionFuture, sql));
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
        this.affectedRow = 0;
        this.lastInsertId = 0;
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> sendOk(long affectedRow) {
        this.affectedRow = affectedRow;
        this.lastInsertId = 0;
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> sendOk(long affectedRow, long lastInsertId) {
        this.affectedRow = affectedRow;
        this.lastInsertId = lastInsertId;
        return Future.succeededFuture();
    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        return null;
    }

    @Override
    public Future<Void> swapBuffer(Observable<PacketRequest> sender, Emitter<PacketResponse> recycler) {
        return null;
    }

    @Override
    public Future<Void> sendVectorResultSet(MycatRowMetaData mycatRowMetaData,
                                            Observable<VectorSchemaRoot> observable) {
        return null;
    }

    @Override
    public Future<Void> proxyProcedure(String sql, String targetName) {
        return null;
    }

    @Override
    public Future<Void> rollbackSavepoint(String name) {
        return null;
    }

    @Override
    public Future<Void> setSavepoint(String name) {
        return null;
    }

    @Override
    public Future<Void> releaseSavepoint(String name) {
        return null;
    }

    @Override
    public Future<Void> sendResultSet(Observable<MysqlPayloadObject> mysqlPacketObservable) {
        Iterable<MysqlPayloadObject> mysqlPayloadObjects = mysqlPacketObservable.blockingIterable();
        Iterator<MysqlPayloadObject> iterator = mysqlPayloadObjects.iterator();
        this. metaData = null;
        while (iterator.hasNext()) {
            MysqlPayloadObject payloadObject = iterator.next();
            if (payloadObject instanceof MysqlObjectArrayRow) {
                throw new UnsupportedOperationException();
            } else if (payloadObject instanceof MySQLColumnDef) {
                metaData = ((MySQLColumnDef) payloadObject).getMetaData();
                break;
            }
        }
        Iterator<Object[]> transform = Iterators.transform(iterator, (i) -> ((MysqlObjectArrayRow) i).getRow());
        Promise<Object> promise = Promise.promise();


        class RowCountIterator implements Iterator<Object[]> {
            long rowCount = 0;
            @Override
            public boolean hasNext() {
                try {
                    boolean b = transform.hasNext();
                    if (!b) {
                        promise.complete();
                    }
                    return b;
                } catch (Exception e) {
                    promise.fail(e);
                    return false;
                }
            }

            @Override
            public Object[] next() {
                try {
                    rowCount++;
                    return transform.next();
                } catch (Exception e) {
                    promise.fail(e);
                    throw e;
                }
            }
        };
        RowCountIterator rowCountIterator = new RowCountIterator();
        this.iterator = rowCountIterator;
        return promise.future().onComplete(new Handler<AsyncResult<Object>>() {
            @Override
            public void handle(AsyncResult<Object> event) {
                //dataContext.setAffectedRows(rowCountIterator.rowCount);
            }
        }).mapEmpty();
    }
}

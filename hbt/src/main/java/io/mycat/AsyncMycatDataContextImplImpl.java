package io.mycat;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIteratorCloseCallback;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.sqlrecorder.SqlRecord;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlString;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.mycat.calcite.executor.MycatPreparedStatementUtil.executeQuery;

public class AsyncMycatDataContextImplImpl extends NewMycatDataContextImpl {
    public AsyncMycatDataContextImplImpl(MycatDataContext dataContext, CodeExecuterContext context, List<Object> params, boolean forUpdate) {
        super(dataContext, context, params, forUpdate);
    }

    public  final  static ReentrantLock lock = new ReentrantLock();
    private final IdentityHashMap<Object,  List<  Future<Observable> >> viewMap = new IdentityHashMap<>();
    public Future allocateResource() {
        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
//        List<Future> list = new ArrayList<>();
//        if (dataContext.isInTransaction()) {
//            lock.lock();
//            CodeExecuterContext.JdbcConnectionUsage jdbcConnectionUsage
//                    = codeExecuterContext.computeTargetConnection(dataContext, params);
//            jdbcConnectionUsage.getJDBCConnection()
//            Set<String> strings = stringIntegerMap.keySet();
//            for (String string : strings) {
//                list.add(transactionSession.getConnection(dataContext.resolveDatasourceTargetName(string,true)))
//            }
//            return CompositeFuture.all(list).eventually((Function<Void, Future<Void>>) unused -> {
//                lock.unlock();
//                return Future.succeededFuture();
//            });
//        }
        return Future.succeededFuture();
    }

    private Future onParellel(MycatView mycatView) {
       return onHeap(mycatView);
    }

    private Future onHeap(MycatView mycatView) {
        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
        Future<Void> objectFuture = Future.succeededFuture();
        List<  Future<Observable<Object[]>> > list = new ArrayList<>();
        for (Map.Entry<String, SqlString> entry : mycatView.expandToSql(forUpdate, params).entries()) {
            String target = dataContext.resolveDatasourceTargetName(entry.getKey());
            objectFuture =   objectFuture.map(unused -> {
                Future<SqlConnection> connection = transactionSession.getConnection(target);
                Future<Observable<Object[]>> rowObservableFuture = (Future)VertxExecuter.runQuery(connection, entry.getKey(), params);
                Future<Observable<Object[]>> map = rowObservableFuture.map(observable -> Observable.fromIterable(observable.blockingIterable()));
                list.add(map);
                return null;
            });

        }
        CompositeFuture all = CompositeFuture.all((List) list);
        synchronized (viewMap){
            viewMap.put(mycatView,(List)list);
        }
        return all;
    }
    public void run() {
        TransactionSession transactionSession = dataContext.getTransactionSession();
        for (RelNode relNode : codeExecuterContext.getMycatViews()) {
            if (relNode instanceof MycatView) {
                MycatView mycatView = (MycatView) relNode;
                if (transactionSession.isInTransaction()) {
                    onHeap(mycatView);
                } else {
                    onParellel(mycatView);
                } } else {
                throw new UnsupportedOperationException("unsupported:" + relNode);
            }
        }
    }

    @Override
    public Enumerable<Object[]> getEnumerable(RelNode node) {
        return Linq4j.asEnumerable(Observable.concat(getObservables(node)).blockingIterable());
    }

    @Override
    public List<Enumerable<Object[]>> getEnumerables(RelNode node) {
        return getObservables(node).stream().map(i -> Linq4j.asEnumerable(i.blockingIterable())).collect(Collectors.toList());
    }

    @Override
    public Observable<Object[]> getObservable(RelNode node) {
        return (Observable<Object[]>) viewMap.get(node).get(0);
    }

    @Override
    public List<Observable<Object[]>> getObservables(RelNode node) {
        return (List) viewMap.get(node);
    }
}

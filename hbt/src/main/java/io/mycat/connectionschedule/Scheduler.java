package io.mycat.connectionschedule;


import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.MycatSqlDialect;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatMergeSort;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.sqlclient.SqlConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public  class Scheduler implements Runnable {
    private final static Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);
    private final PriorityBlockingQueue<SubTask> queue = new PriorityBlockingQueue<>();
    private long timeout;

    public Scheduler(long timeout) {
        this.timeout = timeout;
    }


    public static Future<Void> addTask(Future<SqlConnection> connectionFuture,
                                      @NonNull ObservableEmitter<Object[]> emitter,
                                      SqlString sqlString,
                                      List<Object> params,
                                      CalciteRowMetaData calciteRowMetaData,
                                      AtomicBoolean cancel) {
        Future<Void> closeFuture = Future.future(promise -> {
            Observable<Object[]> observable = Objects.requireNonNull(VertxExecuter.runQuery(connectionFuture,
                    sqlString.getSql(),
                    MycatPreparedStatementUtil.extractParams(params, sqlString.getDynamicParameters()), calciteRowMetaData));
            observable.subscribe(objects -> {
                        if (cancel.get()) {
                            return;
                        }
                        emitter.onNext(objects);
                    },
                    throwable -> {
                        promise.tryFail(throwable);
                    }, () -> {
                        emitter.onComplete();
                        promise.tryComplete();
                    });
        });
        return closeFuture;
    }

    @Override
    public void run() {
        while (true) {
            SubTask subTask = null;
            try {

                subTask = queue.take();
                MySQLManager mySQLManager = MetaClusterCurrent.wrapper(MySQLManager.class);
                ReplicaSelectorManager selector = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
                if (subTask != null) {
                    SubTask curTask = subTask;
                    String datasourceName = selector.getDatasourceNameByReplicaName(curTask.getTarget(), false, null);
                    Future<SqlConnection> connectionFuture = mySQLManager.getConnection(datasourceName);
                    connectionFuture
                            .onSuccess(sqlConnection -> curTask.getPromise().complete(sqlConnection))
                            .onFailure(throwable -> {
                                long now = System.currentTimeMillis();
                                if (curTask.getDeadline() >= now) {
                                    queue.add(curTask);
                                } else {
                                    curTask.getPromise().tryFail("timeout");
                                }
                            });
                    subTask = null;
                }
            } catch (Throwable throwable) {
                LOGGER.error("Scheduler error", throwable);
                if (subTask != null) {
                    subTask.getPromise().tryFail(throwable);
                }
            }
        }

    }

    private static ImmutableMultimap<String, SqlString> expand(RelNode relNode,
                                                               List<Object> params,
                                                               CodeExecuterContext executerContext) {
        if (relNode instanceof MycatView) {
            MycatView mycatView = (MycatView) relNode;
            return mycatView.expandToSql(executerContext.isForUpdate(), params);
        } else if (relNode instanceof MycatTransientSQLTableScan) {
            MycatTransientSQLTableScan transientSQLTableScan = (MycatTransientSQLTableScan) relNode;
            return ImmutableMultimap.of(transientSQLTableScan.getTargetName(),
                    new SqlString(MycatSqlDialect.DEFAULT, transientSQLTableScan.getSql()));
        }else if (relNode instanceof MycatMergeSort) {
            MycatView input =(MycatView) ((MycatMergeSort) relNode).getInput();
         return expand(input, params, executerContext);
        }  else {
            throw new UnsupportedOperationException();
        }
    }
    public Future<IdentityHashMap<RelNode, List<Observable<Object[]>>>> schedule(MycatDataContext context,
                                                                                                        List<Object> params,
                                                                                                        CodeExecuterContext executerContext) {
        XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
       return transactionSession.isInTransaction()?
               computeTargetConnectionByTranscation(context,params,executerContext):
               Future.succeededFuture(computeTargetConnectionByFree(context,params,executerContext));
    }
    private Future<IdentityHashMap<RelNode, List<Observable<Object[]>>>> computeTargetConnectionByTranscation(MycatDataContext context,
                                                                                                        List<Object> params,
                                                                                                        CodeExecuterContext executerContext) {
        XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
        Map<RelNode, Integer> mycatViews = executerContext.getMycatViews();
        Set<String> targets = getTargets(params, executerContext, mycatViews);
        ArrayList<Future> futures = new ArrayList<>();
        for (String target : targets) {
            futures.add(transactionSession.getConnection(context.resolveDatasourceTargetName(target,true)));
        }
        return CompositeFuture.all(futures).map(compositeFuture -> computeTargetConnectionByFree(context, params, executerContext));
    }


    private static Set<String> getTargets(List<Object> params, CodeExecuterContext executerContext, Map<RelNode, Integer> mycatViews) {
        return mycatViews.keySet().stream().map(r -> expand(r, params, executerContext)).flatMap(c -> c.keySet().stream()).collect(Collectors.toSet());
    }

    private IdentityHashMap<RelNode, List<Observable<Object[]>>> computeTargetConnectionByFree(MycatDataContext context,
                                                                                         List<Object> params,
                                                                                         CodeExecuterContext executerContext) {

        SchedulePolicy schedulePolicy = !context.isInTransaction() ?
                (xaSqlConnection, order, refCount, target, deadline, recycleConnectionFuture) -> {
                    recycleConnectionFuture.onSuccess(sqlConnection -> sqlConnection.close());
                    Scheduler scheduler = MetaClusterCurrent.wrapper(Scheduler.class);
                    return Future.future(promise -> scheduler.queue.add(new SubTask(order, refCount, context.resolveDatasourceTargetName(target), promise, deadline)));
                } :
                new SequenceSchedulePolicy();
        Map<RelNode, Integer> mycatViews = executerContext.getMycatViews();
        IdentityHashMap<RelNode, List<Observable<Object[]>>> observableIdentityHashMap = new IdentityHashMap<>();
        for (Map.Entry<RelNode, Integer> e : mycatViews.entrySet()) {
            RelNode relNode = e.getKey();
            Integer refCount = e.getValue();
            List<Observable<Object[]>> observables = schedule(
                    schedulePolicy,
                    context,
                    relNode,
                    refCount,
                    params, executerContext);
            if (refCount > 1) {
                observables = observables.stream().map(s->s.share()).collect(Collectors.toList());
            }
            observableIdentityHashMap.put(relNode, observables);
        }
        return observableIdentityHashMap;
    }

    private List<Observable<Object[]>> schedule(SchedulePolicy schedulePolicy,
                                                MycatDataContext context,
                                          RelNode relNode,
                                          int refCount,
                                          List<Object> params,
                                          CodeExecuterContext executerContext) {
        return schedule(schedulePolicy, context, relNode, refCount, params, executerContext, System.currentTimeMillis() + timeout);
    }

    private List<Observable<Object[]>> schedule(SchedulePolicy schedulePolicy,
                                                MycatDataContext context,
                                          RelNode relNode,
                                          int refCount,
                                          List<Object> params,
                                          CodeExecuterContext executerContext,
                                          long deadline) {
        CalciteRowMetaData calciteRowMetaData = new CalciteRowMetaData(relNode.getRowType().getFieldList());
        ImmutableMultimap<String, SqlString> sqls = expand(relNode, params, executerContext);
        List<Observable<Object[]>> observables = new ArrayList<>(sqls.size());
        int order = 0;
        for (Map.Entry<String, SqlString> e : sqls.entries()) {
            String target = e.getKey();
            SqlString sqlString = e.getValue();
            observables.add(schedule(schedulePolicy, context, order, refCount, target, deadline, sqlString, params, calciteRowMetaData));
            order++;
        }
        return observables;
    }

    private Observable<Object[]> schedule(SchedulePolicy schedulePolicy,
                                          MycatDataContext context,
                                          int order, int refCount, String target, long deadline,
                                          SqlString sqlString,
                                          List<Object> params,
                                          CalciteRowMetaData calciteRowMetaData) {
        return Observable.create(emitter -> {
            Promise<Void> resultSetClosePromise = VertxUtil.newPromise();
            Promise<SqlConnection> sqlConnectionRecyclePromise = VertxUtil.newPromise();
            emitter.setCancellable(() -> resultSetClosePromise.tryComplete());
            Future<Void> resultSetCloseFuture = resultSetClosePromise.future();
            Future<SqlConnection> connectionFuture = schedulePolicy
                    .getConnetion(context, order, refCount, (target), deadline, sqlConnectionRecyclePromise.future());
            AtomicBoolean cancel = new AtomicBoolean(false);
            Future<Void> closeFuture = addTask(connectionFuture,
                    emitter, sqlString, params, calciteRowMetaData,cancel);
            closeFuture.onFailure(event -> emitter.tryOnError(event));
            closeFuture.onSuccess(event -> emitter.onComplete());
            XaSqlConnection xaSqlConnection = (XaSqlConnection)context.getTransactionSession();
            xaSqlConnection.addCloseFuture(resultSetCloseFuture.eventually(unused ->
            {cancel.set(true);return closeFuture;})
                    .onComplete(voidAsyncResult -> connectionFuture.onComplete(sqlConnectionRecyclePromise)));
        });
    }
}
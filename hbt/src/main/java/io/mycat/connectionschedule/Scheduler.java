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
package io.mycat.connectionschedule;


import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.ReplicaBalanceType;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.MycatSqlDialect;
import io.mycat.calcite.Rel;
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
                                       MycatRowMetaData calciteRowMetaData,
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
                    String datasourceName = selector.getDatasourceNameByReplicaName(curTask.getTarget(), false, ReplicaBalanceType.NONE,null);
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

    private static ImmutableMultimap<String, SqlString> expand(String relNode,
                                                               List<Object> params,
                                                               CodeExecuterContext executerContext) {
       return executerContext.expand(relNode,params);
//        if (relNode instanceof MycatView) {
//            MycatView mycatView = (MycatView) relNode;
//            return null;
//        } else if (relNode instanceof MycatTransientSQLTableScan) {
//            MycatTransientSQLTableScan transientSQLTableScan = (MycatTransientSQLTableScan) relNode;
//            return ImmutableMultimap.of(transientSQLTableScan.getTargetName(),
//                    new SqlString(MycatSqlDialect.DEFAULT, transientSQLTableScan.getSql()));
//        }else if (relNode instanceof MycatMergeSort) {
//            MycatView input =(MycatView) ((MycatMergeSort) relNode).getInput();
//         return expand(input, params, executerContext);
//        }  else {
//            throw new UnsupportedOperationException();
//        }
    }
    public Future<Map<String, List<Observable<Object[]>>>> schedule(MycatDataContext context,
                                                                                                        List<Object> params,
                                                                                                        CodeExecuterContext executerContext) {
        XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
       return transactionSession.isInTransaction()?
               computeTargetConnectionByTranscation(context,params,executerContext):
               Future.succeededFuture(computeTargetConnectionByFree(context,params,executerContext));
    }
    private Future<Map<String, List<Observable<Object[]>>>> computeTargetConnectionByTranscation(MycatDataContext context,
                                                                                                        List<Object> params,
                                                                                                        CodeExecuterContext executerContext) {
        XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
        Map<String, Rel> nodes = executerContext.getRelContext().nodes;
        Set<String> targets = getTargets(params, executerContext, nodes.keySet());
        ArrayList<Future> futures = new ArrayList<>();
        for (String target : targets) {
            futures.add(transactionSession.getConnection(context.resolveDatasourceTargetName(target,true)));
        }
        return CompositeFuture.all(futures).map(compositeFuture -> computeTargetConnectionByFree(context, params, executerContext));
    }


    private static Set<String> getTargets(List<Object> params, CodeExecuterContext executerContext, Set<String> mycatViews) {
        return mycatViews.stream().map(r -> expand(r, params, executerContext)).flatMap(c -> c.keySet().stream()).collect(Collectors.toSet());
    }

    private Map<String, List<Observable<Object[]>>> computeTargetConnectionByFree(MycatDataContext context,
                                                                                         List<Object> params,
                                                                                         CodeExecuterContext executerContext) {

        SchedulePolicy schedulePolicy = !context.isInTransaction() ?
                (xaSqlConnection, order, refCount, target, deadline, recycleConnectionFuture) -> {
                    recycleConnectionFuture.onSuccess(sqlConnection -> sqlConnection.close());
                    Scheduler scheduler = MetaClusterCurrent.wrapper(Scheduler.class);
                    return Future.future(promise -> scheduler.queue.add(new SubTask(order, refCount, context.resolveDatasourceTargetName(target), promise, deadline)));
                } :
                new SequenceSchedulePolicy();
        Map<String, Rel> nodes = executerContext.getRelContext().nodes;
        Map<String, List<Observable<Object[]>>> observableIdentityHashMap = new HashMap<>();
        for (Map.Entry<String, Rel> e : nodes.entrySet()) {
            String relNode = e.getKey();
            int refCount = e.getValue().count;
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
                                          String relNode,
                                          int refCount,
                                          List<Object> params,
                                          CodeExecuterContext executerContext) {
        return schedule(schedulePolicy, context, relNode, refCount, params, executerContext, System.currentTimeMillis() + timeout);
    }

    private List<Observable<Object[]>> schedule(SchedulePolicy schedulePolicy,
                                                MycatDataContext context,
                                                String relNode,
                                          int refCount,
                                          List<Object> params,
                                          CodeExecuterContext executerContext,
                                          long deadline) {
        MycatRowMetaData calciteRowMetaData = executerContext.get(relNode);
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
                                          MycatRowMetaData calciteRowMetaData) {
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
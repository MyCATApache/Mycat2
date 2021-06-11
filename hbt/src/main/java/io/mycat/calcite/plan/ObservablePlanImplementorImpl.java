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
package io.mycat.calcite.plan;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import hu.akarnokd.rxjava3.operators.Flowables;
import hu.akarnokd.rxjava3.parallel.ParallelTransformers;
import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.MysqlRow;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.spm.Plan;
import io.mycat.connectionschedule.Scheduler;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.parallel.ParallelFlowable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.util.SqlString;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class ObservablePlanImplementorImpl implements PlanImplementor {
    private final static Logger LOGGER = LoggerFactory.getLogger(ObservablePlanImplementorImpl.class);
    private final XaSqlConnection xaSqlConnection;
    private final MycatDataContext context;
    private final List<Object> params;
    private final Response response;

    public ObservablePlanImplementorImpl(XaSqlConnection xaSqlConnection, MycatDataContext context, List<Object> params, Response response) {
        this.xaSqlConnection = xaSqlConnection;
        this.context = context;
        this.params = params;
        this.response = response;
    }

    @Override
    public Future<Void> executeUpdate(Plan mycatUpdateRel) {
        Future<long[]> future = VertxExecuter.runMycatUpdateRel(xaSqlConnection, context, mycatUpdateRel.getUpdatePhysical(), params);
        return future.eventually(u -> context.getTransactionSession().closeStatementState())
                .flatMap(result -> response.sendOk(result[0], result[1]));
    }

    @Override
    public Future<Void> executeInsert(Plan logical) {
        Future<long[]> future = innerExecuteInsert(logical.getInsertPhysical());
        return future.eventually(u -> context.getTransactionSession().closeStatementState())
                .flatMap(result -> response.sendOk(result[0], result[1]));
    }

    public Future<long[]> innerExecuteInsert(MycatInsertRel logical) {
        return VertxExecuter.runMycatInsertRel(xaSqlConnection, context, logical, params);
    }

    @Override
    public Future<Void> executeQuery(Plan plan) {
        Observable<MysqlPayloadObject> rowObservable = getMysqlPayloadObjectObservable(context, params, plan);
        return response.sendResultSet(rowObservable);
    }

    @NotNull
    public static Observable<MysqlPayloadObject> getMysqlPayloadObjectObservable(MycatDataContext context, List<Object> params, Plan plan) {
        Observable<MysqlPayloadObject> rowObservable = Observable.<MysqlPayloadObject>create(emitter -> {
            emitter.onNext(new MySQLColumnDef(plan.getMetaData()));
            CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
            ArrayBindable bindable = codeExecuterContext.getBindable();
            XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
            final Map<String, Future<SqlConnection>> usedConnnectionMap = new HashMap<>();
            try {
                AsyncMycatDataContextImpl newMycatDataContext =
                        new AsyncMycatDataContextImpl(context, codeExecuterContext, params, plan.forUpdate()) {

                            @Override
                            public Observable<Object[]> getObservable(String node, Function1 function1, Comparator comparator, int offset, int fetch) {
                                List<Observable<Object[]>> observableList = getObservableList(node);
                                Iterable<Flowable<Object[]>> collect = observableList.stream().map(s -> Flowable.fromObservable(s, BackpressureStrategy.BUFFER)).collect(Collectors.toList());
                                Flowable<Object[]> flowable = Flowables.orderedMerge(collect, (o1, o2) -> {
                                    Object left = function1.apply(o1);
                                    Object right = function1.apply(o2);
                                    return comparator.compare(left, right);
                                });
                                if (offset > 0) {
                                    flowable = flowable.skip(offset);
                                }
                                if (fetch > 0 && fetch != Integer.MAX_VALUE) {
                                    flowable = flowable.take(fetch);
                                }
                                return flowable.toObservable();
                            }

                            public List<Observable<Object[]>> getObservableList(String node) {
                                ImmutableMultimap<String, SqlString> expand = this.codeExecuterContext.expand(node, params);
                                MycatRowMetaData calciteRowMetaData = this.codeExecuterContext.get(node);

                                LinkedList<Observable<Object[]>> observables = new LinkedList<>();
                                for (Map.Entry<String, SqlString> entry : expand.entries()) {
                                    String key = context.resolveDatasourceTargetName(entry.getKey());
                                    SqlString sqlString = entry.getValue();
                                    Observable<Object[]> observable = Observable.create(emitter -> {
                                        synchronized (usedConnnectionMap) {
                                            Future<SqlConnection> sessionConnection = usedConnnectionMap
                                                    .computeIfAbsent(key, s -> transactionSession.getConnection(key));
                                            PromiseInternal<SqlConnection> promise = VertxUtil.newPromise();
                                            Observable<Object[]> innerObservable = Objects.requireNonNull(VertxExecuter.runQuery(sessionConnection,
                                                    sqlString.getSql(),
                                                    MycatPreparedStatementUtil.extractParams(params, sqlString.getDynamicParameters()), calciteRowMetaData));
                                            innerObservable.subscribe(objects -> {
                                                        emitter.onNext((objects));
                                                    },
                                                    throwable -> {
                                                        sessionConnection.onSuccess(c -> {
                                                            promise.fail(throwable);
                                                        })
                                                                .onFailure(t -> promise.fail(t));
                                                    }, () -> {
                                                        sessionConnection.onSuccess(c -> {
                                                            promise.tryComplete(c);
                                                        }).onFailure(t -> promise.fail(t));
                                                        ;
                                                    });
                                            usedConnnectionMap.put(key,
                                                    promise.future()
                                                            .onSuccess(c -> {
                                                                emitter.onComplete();
                                                            })
                                                            .onFailure(t -> {
                                                                emitter.onError(t);
                                                            }));
                                        }
                                    });
                                    observables.add(observable);
                                }
                                return observables;
                            }

                            @Override
                            public Observable<Object[]> getObservable(String node) {
                                return Observable.merge(getObservableList(node));
                            }
                        };
                Object bindObservable;
                bindObservable = bindable.bindObservable(newMycatDataContext);
                Observable<Object[]> observable;
                CompositeFuture compositeFuture = CompositeFuture.all(new ArrayList<>(usedConnnectionMap.values()));
                compositeFuture.eventually(unused -> transactionSession.closeStatementState());
                if (bindObservable instanceof Observable) {
                    observable = (Observable) bindObservable;
                } else {
                    Enumerable<Object[]> enumerable = (Enumerable) bindObservable;
                    observable = toObservable(compositeFuture, enumerable);
                }
                observable.subscribe(objects -> emitter.onNext(new MysqlRow(objects)),
                        throwable -> emitter.onError(throwable), () -> {
                            compositeFuture.onSuccess(event -> emitter.onComplete());
                            compositeFuture.onFailure(event -> emitter.onError(event));
                        });
            } catch (Throwable throwable) {
                emitter.onError(throwable);
            }
        });


        return rowObservable;
    }

    @NotNull
    private static Observable<Object[]> toObservable(CompositeFuture compositeFuture, Enumerable<Object[]> enumerable) {
        Observable<Object[]> observable;
        observable = Observable.create(emitter1 -> {
            Future future;
            try (Enumerator<Object[]> enumerator = enumerable.enumerator()) {
                while (enumerator.moveNext()) {
                    emitter1.onNext(enumerator.current());
                }
                future = Future.succeededFuture();
            } catch (Throwable throwable) {
                future = Future.failedFuture(throwable);
            }
            CompositeFuture.all(future, compositeFuture)
                    .onSuccess(event -> emitter1.onComplete())
                    .onFailure(event -> emitter1.onError(event));
        });
        return observable;
    }
}

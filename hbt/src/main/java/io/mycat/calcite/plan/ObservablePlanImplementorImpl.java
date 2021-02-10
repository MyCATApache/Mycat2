package io.mycat.calcite.plan;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.*;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.MysqlRow;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.JdbcConnectionUsage;
import io.mycat.calcite.ProxyConnectionUsage;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.spm.Plan;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.util.RxBuiltInMethodImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class ObservablePlanImplementorImpl implements PlanImplementor {
    private final static Logger LOGGER = LoggerFactory.getLogger(ObservablePlanImplementorImpl.class);
    private XaSqlConnection xaSqlConnection;
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
    public PromiseInternal<Void> execute(MycatUpdateRel mycatUpdateRel) {
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        Future<long[]> future = VertxExecuter.runMycatUpdateRel(xaSqlConnection, context, mycatUpdateRel, params);
        future.onComplete(event -> {
            if (event.succeeded()) {
                long[] result = event.result();
                promise.tryComplete();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.info("sendOk " + Arrays.toString(result));
                }
                response.sendOk(result[0], result[1]);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.error("sendError ", event.cause());
                }
                promise.fail(event.cause());
                response.sendError(event.cause());
            }
        });
        return promise;
    }

    @Override
    public PromiseInternal<Void> execute(MycatInsertRel logical) {
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        Future<long[]> future = VertxExecuter.runMycatInsertRel(xaSqlConnection, context, logical, params);
        future.onComplete(event -> {
            if (event.succeeded()) {
                long[] result = event.result();
                promise.tryComplete();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.info("sendOk " + Arrays.toString(result));
                }
                response.sendOk(result[0], result[1]);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.error("sendError ", event.cause());
                }
                promise.fail(event.cause());
                response.sendError(event.cause());
            }
        });
        return promise;
    }

    @Override
    public PromiseInternal<Void> execute(Plan plan) {
        Observable<MysqlPayloadObject> rowObservable = Observable.<MysqlPayloadObject>create(emitter -> {
            emitter.onNext(new MySQLColumnDef(plan.getMetaData()));
            CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
            ArrayBindable bindable = codeExecuterContext.getBindable();
            ProxyConnectionUsage proxyConnectionUsage = JdbcConnectionUsage.computeProxyTargetConnection(context, params, codeExecuterContext);
            Future<IdentityHashMap<RelNode, List<Observable<Object[]>>>> future = proxyConnectionUsage.collect(xaSqlConnection, params);
            future.onSuccess(relNodeListIdentityHashMap -> {

                    MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
                    mycatWorkerProcessor.getMycatWorker()
                            .execute(()->{
                                try {
                                    IdentityHashMap<RelNode, List<Observable<Object[]>>> map = relNodeListIdentityHashMap;
                                    AsyncMycatDataContextImplImpl newMycatDataContext =
                                            new AsyncMycatDataContextImplImpl(context, codeExecuterContext, (IdentityHashMap) map, params, plan.forUpdate());
                                    Object bindObservable;
//                            if(codeExecuterContext.getCode().contains("hashJoin(org")){
//                                bindObservable = bindObservable(newMycatDataContext);
//                            }else {
                                    bindObservable = bindable.bindObservable(newMycatDataContext);
//                            }
                                    Observable<Object[]> observable;
                                    if (bindObservable instanceof Observable) {
                                        observable = (Observable) bindObservable;
                                    } else {
                                        Enumerable<Object[]> enumerable = (Enumerable) bindObservable;
                                        List<Object[]> list = enumerable.toList();
                                        observable = Observable.fromIterable(list);
                                    }
                                    observable.subscribe(objects -> emitter.onNext(new MysqlRow(objects)),
                                            throwable -> emitter.onError(throwable), () -> emitter.onComplete());
                                }catch (Throwable throwable){
                                    emitter.onError(throwable);
                                }
                            });
          }).onFailure(event -> emitter.onError(event));
        });
        return response.sendResultSet(rowObservable);
    }

    public Object bindObservable(final org.apache.calcite.runtime.NewMycatDataContext root) {
        final org.apache.calcite.rel.RelNode v0stashed = (org.apache.calcite.rel.RelNode) root.get("v0stashed");
        final org.apache.calcite.rel.RelNode v1stashed = (org.apache.calcite.rel.RelNode) root.get("v1stashed");
        final org.apache.calcite.linq4j.Enumerable _inputEnumerable = root.getEnumerable(v0stashed);
        final org.apache.calcite.linq4j.AbstractEnumerable left = new org.apache.calcite.linq4j.AbstractEnumerable(){
            public org.apache.calcite.linq4j.Enumerator enumerator() {
                return new org.apache.calcite.linq4j.Enumerator(){
                    public final org.apache.calcite.linq4j.Enumerator inputEnumerator = _inputEnumerable.enumerator();
                    public void reset() {
                        inputEnumerator.reset();
                    }

                    public boolean moveNext() {
                        return inputEnumerator.moveNext();
                    }

                    public void close() {
                        inputEnumerator.close();
                    }

                    public Object current() {
                        return new Object[] {
                                (                (Object[]) inputEnumerator.current())[0]};
                    }

                };
            }

        };
        return org.apache.calcite.util.RxBuiltInMethodImpl.toEnumerable(left).hashJoin(org.apache.calcite.util.RxBuiltInMethodImpl.toEnumerable(root.getEnumerable(v1stashed)), new org.apache.calcite.linq4j.function.Function1() {
                    public java.math.BigDecimal apply(Object[] v1) {
                        return v1[0] == null ? (java.math.BigDecimal) null : org.apache.calcite.runtime.SqlFunctions.toBigDecimal(v1[0]);
                    }
                    public Object apply(Object v1) {
                        return apply(
                                (Object[]) v1);
                    }
                }
                , new org.apache.calcite.linq4j.function.Function1() {
                    public Long apply(Object[] v1) {
                        return (Long) v1[0];
                    }
                    public Object apply(Object v1) {
                        return apply(
                                (Object[]) v1);
                    }
                }
                , new org.apache.calcite.linq4j.function.Function2() {
                    public Object[] apply(Object[] left, Object[] right) {
                        return new Object[] {
                                left[0],
                                right[0],
                                right[1],
                                right[2]};
                    }
                    public Object[] apply(Object left, Object right) {
                        return apply(
                                (Object[]) left,
                                (Object[]) right);
                    }
                }
                , null, false, false, null);
    }

}

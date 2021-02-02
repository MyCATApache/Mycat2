package io.mycat.calcite.plan;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.AsyncMycatDataContextImplImpl;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.JdbcConnectionUsage;
import io.mycat.calcite.ProxyConnectionUsage;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.spm.Plan;
import io.mycat.util.CalciteConvertors;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.function.Function;


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
                if (LOGGER.isDebugEnabled()){
                    LOGGER.info("sendOk "+ Arrays.toString(result));
                }
                response.sendOk(result[0], result[1]);
            } else {
                if (LOGGER.isDebugEnabled()){
                    LOGGER.error("sendError ",event.cause());
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
                if (LOGGER.isDebugEnabled()){
                    LOGGER.info("sendOk "+ Arrays.toString(result));
                }
                response.sendOk(result[0], result[1]);
            } else {
                if (LOGGER.isDebugEnabled()){
                    LOGGER.error("sendError ",event.cause());
                }
                promise.fail(event.cause());
                response.sendError(event.cause());
            }
        });
        return promise;
    }

    @Override
    public PromiseInternal<Void> execute(Plan plan) {
        CalciteRowMetaData calciteRowMetaData = new CalciteRowMetaData(plan.getPhysical().getRowType().getFieldList());
        RowObservable rowObservable = new RowObservable() {


            @Override
            public void close() throws IOException {

            }

            @Override
            protected void subscribeActual(@NonNull Observer<? super Object[]> observer) {
                CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
                ArrayBindable bindable = codeExecuterContext.getBindable();

                ProxyConnectionUsage proxyConnectionUsage = JdbcConnectionUsage.computeProxyTargetConnection(context, params, codeExecuterContext);
                Future<IdentityHashMap<RelNode, List<RowObservable>>> collect = proxyConnectionUsage.collect(xaSqlConnection, params);
                collect.map(relNodeListIdentityHashMap -> {
                    AsyncMycatDataContextImplImpl newMycatDataContext =
                            new AsyncMycatDataContextImplImpl(context, codeExecuterContext, (IdentityHashMap) relNodeListIdentityHashMap, params, plan.forUpdate());
                    Object bindObservable = bindable.bindObservable(newMycatDataContext);
                    if (bindObservable instanceof Observable) {
                        Observable<Object[]> observable = (Observable) bindObservable;
                        observable.subscribe(observer);
                    } else {
                        Enumerable<Object[]> observable = (Enumerable) bindObservable;
                        Observable.fromIterable(observable).subscribe(observer);
                    }
                    return null;
                }).onFailure(event -> observer.onError(event));
            }

            @Override
            public MycatRowMetaData getRowMetaData() {
                return calciteRowMetaData;
            }
        };
        return response.sendResultSet(rowObservable);
    }
}

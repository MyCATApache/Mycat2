package io.mycat.calcite.plan;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.MycatDataContext;
import io.mycat.NewMycatDataContextImpl;
import io.mycat.Response;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.spm.Plan;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.CodeExecuterContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ObservablePlanImplementorImpl implements PlanImplementor {
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
            PromiseInternal<Void> voidPromiseInternal;
            if (event.succeeded()) {
                long[] result = event.result();
                voidPromiseInternal = response.sendOk(result[0], result[1]);
            } else {
                voidPromiseInternal = response.sendError(event.cause());
            }
            voidPromiseInternal.future().onComplete(event1 -> promise.handle(event1));
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
                response.sendOk(result[0], result[1]);
            } else {
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

                NewMycatDataContextImpl newMycatDataContext = new NewMycatDataContextImpl(context, codeExecuterContext, params, false);
                newMycatDataContext.allocateResource();
                Object bindObservable = bindable.bindObservable(newMycatDataContext);
                if (bindObservable instanceof Observable){
                    Observable<Object[]> observable = (Observable)bindObservable;
                    List<Object[]> objects = observable.cache().toList().blockingGet();
                    Observable.fromIterable(objects).subscribe(observer);
                }else {
                    Enumerable<Object[]> observable = (Enumerable)bindObservable;
                    Observable.fromIterable(observable).subscribe(observer);
                }
            }

            @Override
            public MycatRowMetaData getRowMetaData() {
                return calciteRowMetaData;
            }
        };
        return response.sendResultSet(rowObservable);
    }
}

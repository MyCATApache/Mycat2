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
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.CodeExecuterContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

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
            if (event.succeeded()) {
                long[] result = event.result();
                response.sendOk(result[0], result[1]).handle(promise);
            } else {

                response.sendError(event.cause()).handle(promise);
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
                response.sendOk(result[0], result[1]).handle(promise);
            } else {

                response.sendError(event.cause()).handle(promise);
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

                NewMycatDataContextImpl newMycatDataContext = new NewMycatDataContextImpl(context, codeExecuterContext, Collections.emptyList(), false);
                newMycatDataContext.allocateResource();
                Observable<Object[]> observable = bindable.bindObservable(newMycatDataContext);
                observable.subscribe(observer);
            }

            @Override
            public MycatRowMetaData getRowMetaData() {
                return calciteRowMetaData;
            }
        };
       return response.sendResultSet(rowObservable);
    }
}

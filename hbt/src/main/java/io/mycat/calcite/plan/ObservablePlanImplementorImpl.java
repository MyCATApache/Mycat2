package io.mycat.calcite.plan;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.*;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.MysqlRow;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.ProxyConnectionUsage;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.spm.Plan;
import io.mycat.connectionschedule.Scheduler;
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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
    public Future<Void> execute(MycatUpdateRel mycatUpdateRel) {
        Future<long[]> future = VertxExecuter.runMycatUpdateRel(xaSqlConnection, context, mycatUpdateRel, params);
        return future.eventually(u->context.getTransactionSession().closeStatementState())
                .flatMap(result-> response.sendOk(result[0], result[1]));
    }

    @Override
    public Future<Void> execute(MycatInsertRel logical) {
        Future<long[]> future = innerExecuteInsert(logical);
        return future.eventually(u->context.getTransactionSession().closeStatementState())
                .flatMap(result-> response.sendOk(result[0], result[1]));
    }

    public Future<long[]> innerExecuteInsert(MycatInsertRel logical) {
        return VertxExecuter.runMycatInsertRel(xaSqlConnection, context, logical, params);
    }

    @Override
    public Future<Void> execute(Plan plan) {
        Observable<MysqlPayloadObject> rowObservable = getMysqlPayloadObjectObservable(context,params,plan);
        return response.sendResultSet(rowObservable);
    }

    @NotNull
    public static Observable<MysqlPayloadObject> getMysqlPayloadObjectObservable(MycatDataContext context,List<Object> params,Plan plan) {
        Observable<MysqlPayloadObject> rowObservable = Observable.<MysqlPayloadObject>create(emitter -> {
            emitter.onNext(new MySQLColumnDef(plan.getMetaData()));
            CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
            ArrayBindable bindable = codeExecuterContext.getBindable();
            Scheduler scheduler = MetaClusterCurrent.wrapper(Scheduler.class);
            Future<IdentityHashMap<RelNode, List<Observable<Object[]>>>> future = scheduler.schedule(context,params,plan.getCodeExecuterContext());
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
        return rowObservable;
    }
}

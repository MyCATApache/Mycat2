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
import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.DrdsSqlWithParams;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.MysqlRow;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.spm.Plan;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;


public class ObservablePlanImplementorImpl implements PlanImplementor {
    private final static Logger LOGGER = LoggerFactory.getLogger(ObservablePlanImplementorImpl.class);
    private final XaSqlConnection xaSqlConnection;
    private final MycatDataContext context;
    private final DrdsSqlWithParams drdsSqlWithParams;
    private final Response response;

    public ObservablePlanImplementorImpl(XaSqlConnection xaSqlConnection, MycatDataContext context, DrdsSqlWithParams drdsSqlWithParams, Response response) {
        this.xaSqlConnection = xaSqlConnection;
        this.context = context;
        this.drdsSqlWithParams = drdsSqlWithParams;
        this.response = response;
    }

    @Override
    public Future<Void> executeUpdate(Plan mycatUpdateRel) {
        Future<long[]> future = VertxExecuter.runMycatUpdateRel(xaSqlConnection, context, mycatUpdateRel.getUpdatePhysical(), drdsSqlWithParams.getParams());
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
        return VertxExecuter.runMycatInsertRel(xaSqlConnection, context, logical, drdsSqlWithParams.getParams());
    }

    @Override
    public Future<Void> executeQuery(Plan plan) {
        AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext = new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(context, plan.getCodeExecuterContext(), drdsSqlWithParams);
        Observable<MysqlPayloadObject> rowObservable = getMysqlPayloadObjectObservable(context, sqlMycatDataContext, plan);
        Optional<Long> timeout = drdsSqlWithParams.getTimeout();
        if (timeout.isPresent()) {
            rowObservable = rowObservable.timeout(timeout.get(), TimeUnit.MILLISECONDS);
        }
        return response.sendResultSet(rowObservable);
    }

    @NotNull
    public static Observable<MysqlPayloadObject> getMysqlPayloadObjectObservable(
            MycatDataContext context,
            AsyncMycatDataContextImpl newMycatDataContext,
            Plan plan) {
        Observable<MysqlPayloadObject> rowObservable = Observable.<MysqlPayloadObject>create(emitter -> {
            emitter.onNext(new MySQLColumnDef(plan.getMetaData()));
            CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
            ArrayBindable bindable = codeExecuterContext.getBindable();
            XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();

            try {

                Object bindObservable;
                bindObservable = bindable.bindObservable(newMycatDataContext);
                Observable<Object[]> observable;
                CompositeFuture compositeFuture = newMycatDataContext.endFuture();
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

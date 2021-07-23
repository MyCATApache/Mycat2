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
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.DrdsSqlWithParams;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.MysqlRow;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.spm.Plan;
import io.mycat.vertx.VertxExecuter;
import io.mycat.vertx.VertxUpdateExecuter;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.runtime.ArrayBindable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


public class ObservablePlanImplementorImpl implements PlanImplementor {
    protected final static Logger LOGGER = LoggerFactory.getLogger(ObservablePlanImplementorImpl.class);
    protected final XaSqlConnection xaSqlConnection;
    protected final MycatDataContext context;
    protected final DrdsSqlWithParams drdsSqlWithParams;
    protected final Response response;

    public ObservablePlanImplementorImpl(XaSqlConnection xaSqlConnection, MycatDataContext context, DrdsSqlWithParams drdsSqlWithParams, Response response) {
        this.xaSqlConnection = xaSqlConnection;
        this.context = context;
        this.drdsSqlWithParams = drdsSqlWithParams;
        this.response = response;
    }

    @Override
    public Future<Void> executeUpdate(Plan plan) {
        MycatUpdateRel mycatRel = (MycatUpdateRel) plan.getMycatRel();
        Collection<VertxExecuter.EachSQL> eachSQLS = VertxUpdateExecuter.explainUpdate(drdsSqlWithParams, context);
        Future<long[]> future = VertxExecuter.simpleUpdate(context, true, mycatRel.isGlobal(), eachSQLS);
        return future.eventually(u -> context.getTransactionSession().closeStatementState())
                .flatMap(result -> response.sendOk(result[0], result[1]));
    }

    @Override
    public Future<Void> executeInsert(Plan logical) {
        MycatInsertRel mycatRel = (MycatInsertRel) logical.getMycatRel();
        Iterable<VertxExecuter.EachSQL> insertSqls = VertxExecuter.explainInsert((SQLInsertStatement) mycatRel.getSqlStatement(), drdsSqlWithParams.getParams());
        ArrayList<VertxExecuter.EachSQL> sqlList= Lists.newArrayList(insertSqls.iterator());
        assert  !sqlList.isEmpty();
        Iterable<VertxExecuter.EachSQL> batchSqls ;
        if (sqlList.size()>1){
            batchSqls = VertxExecuter.rewriteInsertBatchedStatements(sqlList);
        }else {
            batchSqls = insertSqls;
        }
        Future<long[]> future = VertxExecuter.simpleUpdate(context, true, mycatRel.isGlobal(), batchSqls);
        return future.eventually(u -> context.getTransactionSession().closeStatementState())
                .flatMap(result -> response.sendOk(result[0], result[1]));
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
            try {

                Object bindObservable;
                bindObservable = bindable.bindObservable(newMycatDataContext);
                Observable<Object[]> observable;
                if (bindObservable instanceof Observable) {
                    observable = (Observable) bindObservable;
                } else {
                    Enumerable<Object[]> enumerable = (Enumerable) bindObservable;
                    observable = toObservable(newMycatDataContext, enumerable);
                }
                observable.subscribe(objects -> emitter.onNext(new MysqlRow(objects)),
                        throwable -> {
                            newMycatDataContext.endFuture()
                                    .onComplete(event -> emitter.onError(throwable));
                        }, () -> {
                            CompositeFuture compositeFuture = newMycatDataContext.endFuture();
                            compositeFuture.onSuccess(event -> emitter.onComplete());
                            compositeFuture.onFailure(event -> emitter.onError(event));
                        });
            } catch (Throwable throwable) {
                CompositeFuture compositeFuture = newMycatDataContext.endFuture();
                compositeFuture.onComplete(event -> emitter.onError(throwable));
            }
        });
        return rowObservable;
    }

    @NotNull
    private static Observable<Object[]> toObservable(AsyncMycatDataContextImpl context, Enumerable<Object[]> enumerable) {
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
            CompositeFuture.join(future, context.endFuture())
                    .onSuccess(event -> emitter1.onComplete())
                    .onFailure(event -> emitter1.onError(event));
        });
        return observable;
    }
}

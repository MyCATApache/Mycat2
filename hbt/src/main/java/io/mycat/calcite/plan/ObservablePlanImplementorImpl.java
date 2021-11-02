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
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import io.mycat.*;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.calcite.ExecutorProvider;
import io.mycat.calcite.PrepareExecutor;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.spm.Plan;
import io.mycat.vertx.VertxExecuter;
import io.mycat.vertx.VertxUpdateExecuter;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
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
        Future<long[]> future = VertxExecuter.simpleUpdate(context, mycatRel.isInsert(),true, mycatRel.isGlobal(), eachSQLS);
        return future.eventually(u -> context.getTransactionSession().closeStatementState())
                .flatMap(result -> response.sendOk(result[0], result[1]));
    }

    @Override
    public Future<Void> executeInsert(Plan logical) {
        MycatInsertRel mycatRel = (MycatInsertRel) logical.getMycatRel();
        List<VertxExecuter.EachSQL> insertSqls = VertxExecuter.explainInsert((SQLInsertStatement) mycatRel.getSqlStatement(), drdsSqlWithParams.getParams());
        assert !insertSqls.isEmpty();
        Future<long[]> future;
        if (insertSqls.size() > 1) {
            future = VertxExecuter.simpleUpdate(context, true,true, mycatRel.isGlobal(), VertxExecuter.rewriteInsertBatchedStatements(insertSqls));
        } else {
            future = VertxExecuter.simpleUpdate(context, true,false, mycatRel.isGlobal(), insertSqls);
        }
        return future.eventually(u -> context.getTransactionSession().closeStatementState())
                .flatMap(result -> response.sendOk(result[0], result[1]));
    }


    @Override
    public Future<Void> executeQuery(Plan plan) {
        AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext = new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(context, plan.getCodeExecuterContext(), drdsSqlWithParams);
        ExecutorProvider executorProvider = MetaClusterCurrent.wrapper(ExecutorProvider.class);
        PrepareExecutor prepare = executorProvider.prepare(sqlMycatDataContext,plan);
        Observable observable = mapToTimeoutObservable(prepare.getExecutor(), drdsSqlWithParams);
        switch (prepare.getType()) {
            case ARROW: {
                Observable<VectorSchemaRoot> executor = observable;
                return response.sendVectorResultSet(executor);
            }
            case OBJECT: {
                Observable<MysqlPayloadObject> executor = observable;
                return response.sendResultSet(executor);
            }
            default:
                throw new IllegalStateException("Unexpected value: " + prepare.getType());
        }
    }

    public <T> Observable<T> mapToTimeoutObservable(Observable<T> observable, DrdsSqlWithParams drdsSqlWithParams){
        Optional<Long> timeout = drdsSqlWithParams.getTimeout();
        if (timeout.isPresent()) {
          return observable.timeout(timeout.get(), TimeUnit.MILLISECONDS);
        }
        return observable;
    }


}

package io.mycat.hbt4;

import io.mycat.MycatDataContext;
import io.mycat.hbt3.View;
import io.mycat.hbt4.executor.*;
import io.mycat.hbt4.logical.rel.MycatInsertRel;
import io.mycat.hbt4.logical.rel.MycatUpdateRel;
import io.mycat.util.Pair;
import io.mycat.util.Response;

public class ProxyExecutorImplementor extends ResponseExecutorImplementor  {

    public static ProxyExecutorImplementor create(MycatDataContext context, Response response){
        TempResultSetFactory tempResultSetFactory = new TempResultSetFactoryImpl();
        DatasourceFactory datasourceFactory = new DefaultDatasourceFactory(context);
        return new ProxyExecutorImplementor(datasourceFactory,tempResultSetFactory,response);
    }

    public ProxyExecutorImplementor(DatasourceFactory factory,
                                    TempResultSetFactory tempResultSetFactory,
                                    Response response) {
        super(factory, tempResultSetFactory, response);
    }

    @Override
    public void implementRoot(MycatRel rel) {
        if (rel instanceof MycatInsertRel) {
            Executor executor = super.implement((MycatInsertRel) rel);
            MycatInsertExecutor insertExecutor = (MycatInsertExecutor) executor;
            if (insertExecutor.isProxy()) {
                Pair<String, String> pair = insertExecutor.getSingleSql();
                response.proxyUpdate(pair.getKey(), pair.getValue());
            } else {
                runInsert(insertExecutor);
            }
            return;
        }
        if (rel instanceof MycatUpdateRel) {
            Executor executor = super.implement((MycatUpdateRel) rel);
            MycatUpdateExecutor updateExecutor = (MycatUpdateExecutor) executor;
            if (updateExecutor.isProxy()) {
                response.sendOk(updateExecutor.getLastInsertId(), updateExecutor.getAffectedRow());
            } else {
                runUpdate(updateExecutor);
            }
            return;
        }
        if (rel instanceof View) {
            Executor executor = super.implement((View) rel);
            ViewExecutor viewExecutor = (ViewExecutor) executor;
            if (viewExecutor.isProxy()) {
                Pair<String, String> singleSql = viewExecutor.getSingleSql();
                response.proxySelect(singleSql.getKey(), singleSql.getValue());
            } else {
                runQuery(rel,viewExecutor);
            }
            return;
        }
        super.implementRoot(rel);
    }
}
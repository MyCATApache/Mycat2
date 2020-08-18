/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt4;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.hbt3.MycatLookUpView;
import io.mycat.hbt3.View;
import io.mycat.hbt4.executor.*;
import io.mycat.hbt4.logical.rel.MycatInsertRel;
import io.mycat.hbt4.logical.rel.MycatUpdateRel;
import io.mycat.util.Pair;
import io.mycat.util.Response;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;

public class ExecutorImplementorImpl extends BaseExecutorImplementor {
    private final DatasourceFactory factory;
    private final Response response;
    Type type;

    public ExecutorImplementorImpl(MycatContext context,
                                   DatasourceFactory factory,
                                   TempResultSetFactory tempResultSetFactory,
                                   Response response) {
        super(context, tempResultSetFactory);
        this.factory = factory;
        this.response = response;
    }

    public enum Type {
        PROXY,
        JDBC,
        PROXY_JDBC
    }


    @Override
    public void implementRoot(MycatRel rel) {
        Executor executor = rel.implement(this);
        try {
            if (executor instanceof MycatInsertExecutor) {
                MycatInsertExecutor insertExecutor = (MycatInsertExecutor) executor;
                if (insertExecutor.isProxy()) {
                    switch (type) {
                        case PROXY:
                        case PROXY_JDBC:
                            Pair<String, String> pair = insertExecutor.getSingleSql();
                            response.proxyUpdate(pair.getKey(), pair.getValue());
                            return;
                    }
                }
                insertExecutor.open();
                long affectedRow = insertExecutor.affectedRow;
                long lastInsertId = insertExecutor.lastInsertId;
                response.sendOk(lastInsertId, affectedRow);
                return;
            }
            if (executor instanceof MycatUpdateExecutor) {
                MycatUpdateExecutor updateExecutor = (MycatUpdateExecutor) executor;
                if (updateExecutor.isProxy()) {
                    switch (type) {
                        case PROXY:
                        case PROXY_JDBC:
                            Pair<String, String> pair = updateExecutor.getSingleSql();
                            response.proxyUpdate(pair.getKey(), pair.getValue());
                            return;
                    }
                }
                updateExecutor.open();
                long affectedRow = updateExecutor.affectedRow;
                long lastInsertId = updateExecutor.lastInsertId;
                response.sendOk(lastInsertId, affectedRow);
                return;
            }
            if (executor instanceof ViewExecutor) {
                ViewExecutor viewExecutor = (ViewExecutor)executor;
                if (viewExecutor.isProxy()) {
                    switch (type) {
                        case PROXY:
                        case PROXY_JDBC:
                            Pair<String, String> pair = viewExecutor.getSingleSql();
                            response.proxySelect(pair.getKey(), pair.getValue());
                            return;
                    }
                }
            }
            factory.open();
            executor.open();
            RelDataType rowType = rel.getRowType();
            EnumeratorRowIterator rowIterator = new EnumeratorRowIterator(new CalciteRowMetaData(rowType.getFieldList()),
                    Linq4j.asEnumerable(() -> executor.outputObjectIterator()).enumerator(), () -> {
            });
            response.sendResultSet(()->rowIterator);
        }catch (Exception e){
            if (executor!=null){
                executor.close();
            }
            response.sendError(e);
        }
        return ;
    }

    @Override
    public Executor implement(View view) {
        return ViewExecutor.create(view, context.forUpdate, params, factory);
    }

    @Override
    public Executor implement(MycatTransientSQLTableScan tableScan) {
        MycatRowMetaData calciteRowMetaData = new CalciteRowMetaData(tableScan.getRowType().getFieldList());
        return TmpSqlExecutor.create(calciteRowMetaData, tableScan.getTargetName(), tableScan.getSql(), factory);
    }

    @Override
    public Executor implement(MycatLookUpView mycatLookUpView) {
        return MycatLookupExecutor.create(mycatLookUpView.getRelNode(), factory, params);
    }

    @Override
    public Executor implement(MycatInsertRel mycatInsertRel) {
        return MycatInsertExecutor.create(mycatInsertRel, factory, params);
    }

    @Override
    public Executor implement(MycatUpdateRel mycatUpdateRel) {
        return MycatUpdateExecutor.create(mycatUpdateRel.getValues(),
                mycatUpdateRel.getSqlStatement(),
                factory,
                params
        );
    }
}
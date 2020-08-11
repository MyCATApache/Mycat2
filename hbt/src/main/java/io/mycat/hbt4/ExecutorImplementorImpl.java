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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.table.MycatSQLTableScan;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.hbt3.MycatLookUpView;
import io.mycat.hbt3.View;
import io.mycat.hbt4.executor.MycatJdbcExecutor;
import io.mycat.hbt4.executor.MycatLookupExecutor;
import io.mycat.hbt4.executor.TempResultSetFactory;
import org.jetbrains.annotations.NotNull;

public class ExecutorImplementorImpl extends BaseExecutorImplementor {
    private final DatasourceFactory factory;

    public ExecutorImplementorImpl(MycatContext context,
                                   DatasourceFactory factory,
                                   TempResultSetFactory tempResultSetFactory) {
        super(context,tempResultSetFactory);
        this.factory = factory;
    }


    @Override
    public Executor implement(View view) {
        return factory.create(new CalciteRowMetaData(view.getRowType().getFieldList()),view.expandToSql(false));
    }

    @Override
    public Executor implement(MycatTransientSQLTableScan mycatTransientSQLTableScan) {
        return  MycatJdbcExecutor.create(mycatTransientSQLTableScan.getTable().unwrap(MycatSQLTableScan.class),factory);
    }

    @Override
    public Executor implement(MycatLookUpView mycatLookUpView) {
        return new MycatLookupExecutor(mycatLookUpView.getRelNode(),factory);
    }

    @NotNull
    public Object[] getPzarameters(ImmutableList<Integer> dynamicParameters) {
        Object[] objects;
        if (dynamicParameters != null) {
//            objects = dynamicParameters.stream().map(i -> context.get(i)).toArray();
            objects = null;
        } else {
            objects = new Object[]{};
        }
        return objects;
    }
}
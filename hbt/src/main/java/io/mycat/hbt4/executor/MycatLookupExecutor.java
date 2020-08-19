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
package io.mycat.hbt4.executor;

import com.google.common.collect.ImmutableMultimap;
import io.mycat.MycatWorkerProcessor;
import io.mycat.NameableExecutor;
import io.mycat.api.collector.ComposeFutureRowBaseIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.hbt3.View;
import io.mycat.hbt4.DatasourceFactory;
import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.util.SqlString;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mycat.hbt4.executor.MycatPreparedStatementUtil.executeQuery;

/**
 * it must be on the right of MycatBatchNestedLoopJoinExecutor
 */
public class MycatLookupExecutor implements Executor {

    private final View view;
    private final CalciteRowMetaData metaData;
    private DatasourceFactory factory;
    private List<Object> params;
    private MyCatResultSetEnumerator myCatResultSetEnumerator = null;
    private List<Connection> tmpConnections;

    public MycatLookupExecutor(View view, DatasourceFactory factory, List<Object> params) {
        this.view = view;
        this.factory = factory;
        this.params = params;
        this.metaData = new CalciteRowMetaData(this.view.getRowType().getFieldList());
    }

    public static MycatLookupExecutor create(View view, DatasourceFactory factory, List<Object> params) {
        return new MycatLookupExecutor(view, factory, params);
    }

    void setIn(List<Row> args) {
        if (myCatResultSetEnumerator != null) {
            myCatResultSetEnumerator.close();
            myCatResultSetEnumerator = null;
        }
        if (tmpConnections != null) {
            factory.recycleTmpConnections(tmpConnections);
            tmpConnections = null;
        }
        //convert relNode to sql with cor variable
        RelNode accept = this.view.getRelNode().accept(new RexShuttle() {
            @Override
            public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                RelDataTypeField field = fieldAccess.getField();
                int index = field.getIndex();
                RelDataType type = field.getType();
                if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
                    RexCorrelVariable variable = (RexCorrelVariable) fieldAccess.getReferenceExpr();
                    int id = variable.id.getId();
                    Row row = args.get(id);
                    return MycatCalciteSupport.INSTANCE.RexBuilder.makeLiteral(row.getObject(index), type, false);
                }
                return super.visitFieldAccess(fieldAccess);
            }
        });
        View newView = View.of(accept, this.view.getDistribution());
        ImmutableMultimap<String, SqlString> expandToSqls = newView.expandToSql(false, params);

        MycatWorkerProcessor instance = MycatWorkerProcessor.INSTANCE;
        NameableExecutor mycatWorker = instance.getMycatWorker();
        LinkedList<Future<RowBaseIterator>> futureArrayList = new LinkedList<>();
        this.tmpConnections = factory.getTmpConnections(expandToSqls.keys().asList());
        int i = 0;
        for (Map.Entry<String, SqlString> entry : expandToSqls.entries()) {
            Connection connection = tmpConnections.get(i);
            String target = entry.getKey();
            SqlString sql = entry.getValue();
            futureArrayList.add(mycatWorker.submit(() -> executeQuery(connection, metaData, sql, params)));
            i++;
        }
        AtomicBoolean flag = new AtomicBoolean();
        ComposeFutureRowBaseIterator composeFutureRowBaseIterator = new ComposeFutureRowBaseIterator(metaData, futureArrayList);
        this.myCatResultSetEnumerator = new MyCatResultSetEnumerator(flag, composeFutureRowBaseIterator);
    }

    @Override
    public void open() {

    }

    @Override
    public Row next() {
        if (myCatResultSetEnumerator.moveNext()) {
            return Row.of(myCatResultSetEnumerator.current());
        }
        return null;
    }

    @Override
    public void close() {
        if (this.myCatResultSetEnumerator != null) {
            this.myCatResultSetEnumerator.close();
        }
        if (tmpConnections != null) {
            factory.recycleTmpConnections(tmpConnections);
            tmpConnections = null;
        }
    }

    @Override
    public boolean isRewindSupported() {
        throw new UnsupportedOperationException();
    }
}
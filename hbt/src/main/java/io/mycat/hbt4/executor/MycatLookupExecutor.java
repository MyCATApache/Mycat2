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

import com.google.common.collect.ImmutableMap;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.CalciteRowMetaData;
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

import java.util.List;

/**
 * it must be on the right of MycatBatchNestedLoopJoinExecutor
 */
public class MycatLookupExecutor implements Executor {

    private final View view;
    private final CalciteRowMetaData metaData;
    private DatasourceFactory factory;
    private String currentSql;
    private Executor executor;

    public MycatLookupExecutor(View view, DatasourceFactory factory) {
        this.view = view;
        this.factory = factory;
        this.metaData = new CalciteRowMetaData(this.view.getRowType().getFieldList());
    }

    public static MycatLookupExecutor create(View view, DatasourceFactory factory) {
        return new MycatLookupExecutor(view, factory);
    }

    void setIn(List<Row> args) {
        if (executor!=null){
            executor.close();
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
        this.executor = factory.create(metaData, newView.expandToSql(false));
    }

    @Override
    public void open() {
        this.executor.open();
    }

    @Override
    public Row next() {
        return this.executor.next();
    }

    @Override
    public void close() {
        if (this.executor!=null){
            this.executor.close();
        }
    }

    @Override
    public boolean isRewindSupported() {
       throw new UnsupportedOperationException();
    }
}
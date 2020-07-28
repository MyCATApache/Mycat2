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

import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatSqlDialect;
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

    private final RelNode relNode;
    private String currentSql;

    public MycatLookupExecutor(RelNode relNode) {
        this.relNode = relNode;
    }

    public static Executor create(RelNode relNode) {
        return new MycatLookupExecutor(relNode);
    }

    void setIn(List<Row> args) {
        //convert relNode to sql with cor variable
        RelNode accept = this.relNode.accept(new RexShuttle() {
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
       this.currentSql = MycatCalciteSupport.INSTANCE.convertToSql(accept, MycatSqlDialect.DEFAULT, false);
    }

    @Override
    public void open() {

    }

    @Override
    public Row next() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isRewindSupported() {
        return true;
    }
}
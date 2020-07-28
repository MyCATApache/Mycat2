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

import java.util.HashSet;
import java.util.List;

public class MycatLookupExecutor implements Executor {

    private RelNode relNode;

    public MycatLookupExecutor(RelNode relNode) {
        this.relNode = relNode;
    }

    public static Executor create(RelNode relNode) {
        return new MycatLookupExecutor(relNode);
    }

    void setIn(List<Row> args) {
        HashSet<Row> rows = new HashSet<>(args);
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
        String s = MycatCalciteSupport.INSTANCE.convertToSql(accept, MycatSqlDialect.DEFAULT, false);
        System.out.println(s);
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
        return false;
    }
}
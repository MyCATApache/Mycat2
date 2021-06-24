package io.mycat.calcite.physical;

import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatConvention;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

@Getter
public class MycatTableLookupValues extends AbstractRelNode {

    private final List<RexNode> exprs;

    public MycatTableLookupValues(RelOptCluster cluster, RelDataType relDataType, List<RexNode> exprs, RelTraitSet traitSet) {
        super(cluster, traitSet.replace(MycatConvention.INSTANCE));
        this.exprs = exprs;
        this.traitSet = traitSet;
        RelDataTypeFactory.FieldInfoBuilder builder = MycatCalciteSupport.TypeFactory.builder();
        int index = 0;
        for (RelDataTypeField relDataTypeField : relDataType.getFieldList()) {
            builder.add("column_" + index, relDataTypeField.getType());
            index++;
        }
        this.rowType = builder.build();
    }

    public MycatTableLookupValues(RelInput input) {
        this(input.getCluster(), input.getRowType("type"), input.getExpressionList("tuples"), input.getTraitSet());
    }

    public static MycatTableLookupValues create(RelOptCluster cluster, RelDataType relDataType, List<RexNode> exprs, RelTraitSet traitSet) {
        return new MycatTableLookupValues(cluster, relDataType, exprs, traitSet);
    }

    @Override
    public void explain(RelWriter pw) {
        super.explain(pw);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("tuples", exprs);
    }

    public MycatTableLookupValues apply(List<Object[]> argsList) {
        int size = exprs.size();

        List<RexNode> newExprs = apply(argsList, exprs);
        return new MycatTableLookupValues(getCluster(), getRowType(), newExprs, traitSet);
    }

    @NotNull
    public static LinkedList<RexNode> apply(List<Object[]> argsList, List<RexNode> exprs) {
        RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;
        LinkedList<RexNode> res = new LinkedList<>();
        for (Object[] objects : argsList) {
            List<RexNode> newExprs = new ArrayList<>();
            for (int i = 0; i < exprs.size(); i++) {
                final int index = i;
                RexNode rexNode = exprs.get(i);
                newExprs.add(rexNode.accept(new RexShuttle() {
                    @Override
                    public RexNode visitCall(RexCall call) {
                        if (call.getKind() == SqlKind.CAST) {
                            Object object = objects[index];
                            if (object == null) {
                                return rexBuilder.makeNullLiteral(call.type);
                            }
                           return rexBuilder.makeCast(call.type, rexBuilder.makeLiteral(Objects.toString(object)));
                        }
                        return super.visitCall(call);
                    }
                }));
            }
            res.add(  rexBuilder.makeCall(SqlStdOperatorTable.ROW,newExprs));
        }
        return res;
    }

}

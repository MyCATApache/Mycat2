package io.mycat.mushroom;

import org.apache.calcite.MycatContext;
import org.apache.calcite.rel.type.RelDataType;

public abstract class RexDynamicParamExpression<VFrame> implements CompiledSQLExpression<VFrame> {
    final RelDataType type;
    final MycatContext context;
    final int index;

    public RexDynamicParamExpression(int index, RelDataType type, MycatContext context) {
        this.type = type;
        this.context = context;
        this.index = index;
    }

    @Override
    public Object eval(VFrame vFrame) {
//        return context.getDynamicParam(index);
        return null;
    }
}

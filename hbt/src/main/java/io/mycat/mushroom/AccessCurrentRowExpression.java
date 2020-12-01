package io.mycat.mushroom;

import org.apache.calcite.MycatContext;
import org.apache.calcite.rel.type.RelDataType;

public abstract class AccessCurrentRowExpression<VFrame> implements CompiledSQLExpression<VFrame> {
    final int index;
    final MycatContext context;

    public AccessCurrentRowExpression(int index, MycatContext context, RelDataType type) {
        this.index = index;
        this.context = context;
    }

    @Override
    public Object eval(VFrame vFrame) {
        return readByIndex(vFrame, index);
    }
}

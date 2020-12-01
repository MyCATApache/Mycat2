package io.mycat.mushroom;

import org.apache.calcite.rel.type.RelDataType;

public abstract class CastExpression<VFrame> implements CompiledSQLExpression<VFrame> {
    final RelDataType type;
    public CastExpression(RelDataType type) {
        this.type = type;
    }

    @Override
    public Object eval(VFrame vFrame) {
       return null;
    }
}

package io.mycat.mushroom;

public abstract class AndExpression <VFrame> implements CompiledSQLExpression<VFrame> {
    @Override
    public Object eval(VFrame vFrame) {
        return Boolean.logicalAnd(
                (Boolean) readByIndex(vFrame,0),
                (Boolean) readByIndex(vFrame,1)
        );
    }
}

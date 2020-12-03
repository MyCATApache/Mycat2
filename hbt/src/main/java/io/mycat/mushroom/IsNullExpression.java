package io.mycat.mushroom;

public abstract class IsNullExpression<VFrame> implements CompiledSQLExpression<VFrame> {
    @Override
    public Object eval(VFrame vFrame) {
        return readInput(vFrame) == null;
    }
}

package io.mycat.mushroom;

public class IsNotTrueExpression<VFrame> implements CompiledSQLExpression<VFrame> {
    @Override
    public Object eval(VFrame vFrame) {
        return readInput(vFrame) == null;
    }
}

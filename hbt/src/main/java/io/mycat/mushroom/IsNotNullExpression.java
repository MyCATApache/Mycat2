package io.mycat.mushroom;

public class IsNotNullExpression <VFrame> implements CompiledSQLExpression<VFrame> {
    @Override
    public Object eval(VFrame vFrame) {
        return readInput(vFrame) == null;
    }
}

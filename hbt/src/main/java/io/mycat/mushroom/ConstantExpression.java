package io.mycat.mushroom;

public abstract class ConstantExpression<VFrame> implements CompiledSQLExpression<VFrame> {
    public ConstantExpression(Object value) {
        this.value = value;
    }

    final Object value;

    @Override
    public Object eval(VFrame vFrame) {
        return value;
    }
}

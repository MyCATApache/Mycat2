package io.mycat.mushroom;

public abstract class ConstantNullExpression<VFrame> implements CompiledSQLExpression<VFrame> {

    @Override
    public Object eval(VFrame vFrame) {
        return null;
    }
}

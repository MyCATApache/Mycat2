package io.mycat.mushroom;

public abstract class SignedExpression<VFrame> implements CompiledSQLExpression<VFrame> {


    public SignedExpression() {

    }

    @Override
    public Object eval(VFrame vFrame) {
        return null;
    }
}

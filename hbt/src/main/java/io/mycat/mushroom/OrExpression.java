package io.mycat.mushroom;

public class OrExpression <VFrame> implements CompiledSQLExpression<VFrame> {
    @Override
    public Object eval(VFrame vFrame) {
        return Boolean.logicalOr(
                (Boolean)readByIndex(vFrame,0),
                (Boolean)readByIndex(vFrame,1));
    }
}

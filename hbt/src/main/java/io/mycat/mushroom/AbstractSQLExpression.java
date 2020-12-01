package io.mycat.mushroom;

public abstract class AbstractSQLExpression implements CompiledSQLExpression<Object[]> {
    @Override
    public Object readByIndex(Object[] vFrame, int index) {
        return vFrame[index];
    }
}

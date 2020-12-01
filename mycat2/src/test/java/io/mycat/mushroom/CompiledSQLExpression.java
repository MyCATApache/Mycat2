package io.mycat.mushroom;

public interface CompiledSQLExpression<VFrame> {

    Object eval(VFrame vFrame);

    default Object readByIndex(VFrame vFrame, int index) {
        throw new UnsupportedOperationException();
    }

    default Object readInput(VFrame vFrame) {
        return ((Object[]) vFrame)[0];
    }
}

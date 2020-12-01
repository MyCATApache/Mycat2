package io.mycat.mushroom;

import java.util.Objects;

public  abstract class NotEqualsExpression<VFrame> implements CompiledSQLExpression<VFrame> {
    @Override
    public Object eval(VFrame vFrame) {
        return !Objects.equals(
                readByIndex(vFrame,0),
                readByIndex(vFrame,1)
        );
    }
}

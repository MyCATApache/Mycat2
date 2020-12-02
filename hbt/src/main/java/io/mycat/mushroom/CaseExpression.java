package io.mycat.mushroom;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public  abstract class CaseExpression<VFrame> implements CompiledSQLExpression<VFrame> {
    private final List<Map.Entry<CompiledSQLExpression, CompiledSQLExpression>> cases;
    private final CompiledSQLExpression elseExp;

    public CaseExpression(List<Map.Entry<CompiledSQLExpression, CompiledSQLExpression>> cases,
                          CompiledSQLExpression elseExp) {
        this.cases = cases;
        this.elseExp = elseExp;
    }

    @Override
    public Object eval(VFrame vFrame) {
        return Objects.equals(
                readByIndex(vFrame,0),
                readByIndex(vFrame,1)
        );
    }
}

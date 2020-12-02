package io.mycat.mushroom;

public  abstract class AccessFieldExpression<VFrame> implements CompiledSQLExpression<VFrame> {
    private final String name;
    private final CompiledSQLExpression compileExpression;

    public AccessFieldExpression(String name, CompiledSQLExpression compileExpression) {
        this.name = name;
        this.compileExpression = compileExpression;
    }

    @Override
    public Object eval(VFrame o) {
        return null;
    }
}

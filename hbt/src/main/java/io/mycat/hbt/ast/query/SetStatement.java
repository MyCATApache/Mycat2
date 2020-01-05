package io.mycat.hbt.ast.query;

import io.mycat.hbt.LevelType;
import io.mycat.hbt.ast.base.Node;

public class SetStatement {

    private final LevelType level;
    private final String schema;
    private final String table;
    private final Node expr;

    public SetStatement(LevelType level, String schema, String table, Node expr) {

        this.level = level;
        this.schema = schema;
        this.table = table;
        this.expr = expr;
    }
}
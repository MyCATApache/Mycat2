package cn.lightfish.wu.ast.query;

import cn.lightfish.wu.LevelType;
import cn.lightfish.wu.ast.base.Node;

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
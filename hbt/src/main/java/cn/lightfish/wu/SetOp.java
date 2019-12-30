package cn.lightfish.wu;

import cn.lightfish.wu.ast.base.Identifier;
import cn.lightfish.wu.ast.base.Node;
import cn.lightfish.wu.ast.query.SetStatement;

import static cn.lightfish.wu.BaseQuery.literal;
import static cn.lightfish.wu.LevelType.SESSION;


public interface SetOp {

    static void main(String[] args) {
        set(SESSION, "db1", "travelrecord", literal(1));
    }

    /**
     * @param level
     * @param schema
     * @param table
     * @param expr
     * @return SetStatement
     */
    static SetStatement set(LevelType level, Identifier schema, Identifier table, Node expr) {
        return set(level, schema.getValue(), table.getValue(), expr);
    }

    /**
     * @param level
     * @param schema
     * @param table
     * @param expr
     * @return SetStatement
     */
    static SetStatement set(LevelType level, String schema, String table, Node expr) {
        return new SetStatement(level, schema, table, expr);
    }

    /**
     * @param schema
     * @param table
     * @param expr
     * @return
     */
    static SetStatement set(String schema, String table, Node expr) {
        return set(SESSION, schema, table, expr);
    }

    /**
     * @param expr
     * @param level
     * @param schema
     * @param table
     * @return SetStatement
     */
    static SetStatement set(Node expr, LevelType level, Identifier schema, Identifier table) {
        return set(level, schema, table, expr);
    }

    /**
     * @param expr
     * @param schema
     * @param table
     * @return
     */
    static SetStatement set(Node expr, Identifier schema, Identifier table) {
        return set(SESSION, schema, table, expr);
    }

}
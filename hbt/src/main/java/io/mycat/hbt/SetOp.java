/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.hbt;

import io.mycat.hbt.ast.base.Identifier;
import io.mycat.hbt.ast.base.Node;
import io.mycat.hbt.ast.query.SetStatement;

import static io.mycat.hbt.BaseQuery.literal;
import static io.mycat.hbt.LevelType.SESSION;

/**
 * @author jamie12221
 **/
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
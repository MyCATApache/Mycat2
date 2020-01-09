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

import io.mycat.hbt.ast.base.Node;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.ast.modify.*;

import java.util.Arrays;
import java.util.List;

/**
 * @author jamie12221
 **/
public class ModifyOp extends BaseQuery {
    public static void main(String[] args) {
        update(fromModifyTable("db1", "travelrecord", "id"), delete());
    }


    public static ModifyStatement update(Schema source, List<RowModifer> tables) {
        return new ModifyStatement(source, tables);
    }

    public static ModifyStatement update(Schema source, RowModifer... tables) {
        return update(source, Arrays.asList(tables));
    }

    public static ModifyTable fromModifyTable(String schema, String table, String primaryColumn) {
        return new ModifyTable(schema, table, primaryColumn);
    }

    public static TargetModifyColumn targetModifyColumn(String schema, String table, String columnName) {
        return new TargetModifyColumn(schema, table, columnName);
    }

    public static RowModifer modify(TargetModifyColumn table, Node expr) {
        return new ModifyRowModifer();
    }

    public static RowModifer delete() {
        return new DeleteRowModifer();
    }

    public static RowModifer merge(Node matcher, List<RowModifer> rowModifer, List<Node> values) {
        return new MergeRowModifer(matcher, rowModifer, values);
    }
}
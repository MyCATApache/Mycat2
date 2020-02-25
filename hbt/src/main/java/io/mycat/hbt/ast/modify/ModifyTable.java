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
package io.mycat.hbt.ast.modify;

import io.mycat.hbt.Op;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;

import java.util.List;

/**
 * @author jamie12221
 **/
public class ModifyTable extends Schema {
    private final List<String> names;
    private final List<String> updateColumnList;
    private final Schema schema;

    public ModifyTable(Op op,List<String> names,List<String> updateColumnList,Schema schema) {
        super(op);
        this.schema = schema;
        this.names = names;
        this.updateColumnList = updateColumnList;
    }
    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
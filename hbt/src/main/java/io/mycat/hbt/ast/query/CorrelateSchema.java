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
package io.mycat.hbt.ast.query;

import io.mycat.hbt.ast.HBTOp;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author jamie12221
 **/
@Data
@EqualsAndHashCode(callSuper = true)
public class CorrelateSchema extends Schema {
    private final Schema left;
    private final Schema right;
    private String refName;

    public CorrelateSchema(HBTOp op, String refName, Schema left, Schema right) {
        super(op);
        this.left = left;
        this.right = right;
        this.refName = refName;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
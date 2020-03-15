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

import io.mycat.hbt.Op;
import io.mycat.hbt.ast.base.Node;
import io.mycat.hbt.ast.base.NodeVisitor;
import lombok.Builder;
import lombok.Data;

/**
 * @author jamie12221
 **/
@Data
@Builder
public class FieldType extends Node {
    final String id;
    final String type;
    final boolean nullable;
    final Integer precision;
    final Integer scale;

    public FieldType(String id, String type,boolean columnNullable,Integer precision,Integer scale) {
        super(Op.FIELD_SCHEMA);
        this.id = id;
        this.type = type;
        this.nullable = columnNullable;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}

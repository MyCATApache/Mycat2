/**
 * Copyright (C) <2021>  <chen junwen>
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
import io.mycat.hbt.ast.base.FieldType;
import io.mycat.hbt.ast.base.Literal;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Arrays;
import java.util.List;

/**
 * @author jamie12221
 **/
@Data
@EqualsAndHashCode(callSuper = true)
public class AnonyTableSchema extends Schema {
    private final List<Object> values;
    private final List<FieldType> fieldNames;

    public AnonyTableSchema(List<FieldType> fieldNames, List<Object> values) {
        super(HBTOp.TABLE);
        this.fieldNames = fieldNames;
        this.values = values;
    }

    public AnonyTableSchema(List<FieldType> fieldNames, Literal... values) {
        this(fieldNames, Arrays.asList(values));
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
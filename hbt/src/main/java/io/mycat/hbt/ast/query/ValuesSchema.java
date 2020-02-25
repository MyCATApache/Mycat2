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
import io.mycat.hbt.ast.base.Literal;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Data;

import java.util.Arrays;
import java.util.List;

/**
 * @author jamie12221
 **/
@Data
public class ValuesSchema extends Schema {
    private final List<Object> values;
    private final List<FieldType> fieldNames;

    public ValuesSchema(List<FieldType> fieldNames, List<Object> values) {
        super(Op.TABLE);
        this.fieldNames = fieldNames;
        this.values = values;
    }

    public ValuesSchema(List<FieldType> fieldNames, Literal... values) {
        this(fieldNames, Arrays.asList(values));
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
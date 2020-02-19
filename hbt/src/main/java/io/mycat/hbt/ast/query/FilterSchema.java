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
import io.mycat.hbt.ast.base.Expr;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Data;

import java.util.Collections;
import java.util.List;

/**
 * @author jamie12221
 **/
@Data
public class FilterSchema extends Schema {
    private final Schema schema;
    private final Expr exprs;



    public FilterSchema(Schema schema, Expr exprs) {
        super(Op.FILTER);
        this.schema = schema;
        this.exprs = exprs;
    }

    @Override
    public List<FieldType> fields() {
        return Collections.unmodifiableList(schema.fields());
    }

    public Schema getSchema() {
        return schema;
    }


    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }

    public Expr getExpr() {
        return exprs;
    }

}

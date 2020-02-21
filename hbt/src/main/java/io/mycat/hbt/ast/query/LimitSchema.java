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
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Data;

import java.util.Collections;
import java.util.List;

/**
 * @author jamie12221
 **/
@Data
public class LimitSchema extends Schema {
    private final Schema schema;
    private final Number offset;
    private final Number limit;

    public LimitSchema(Schema schema, Number offset, Number limit) {
        super(Op.LIMIT);
        this.schema = schema;
        this.offset = offset;
        this.limit = limit;
    }

    public Number getOffset() {
        return offset == null ?0 : offset;
    }

    public Number getLimit() {
        return limit;
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
}

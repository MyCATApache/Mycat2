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
import io.mycat.hbt.ast.base.AggregateCall;
import io.mycat.hbt.ast.base.GroupKey;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author jamie12221
 **/
@Data
@EqualsAndHashCode(callSuper = true)
public class GroupBySchema extends Schema {
    private final Schema schema;
    private final List<GroupKey> keys;
    private final List<AggregateCall> exprs;

    public GroupBySchema(Schema schema, List<GroupKey> keys, List<AggregateCall> exprs) {
        super(HBTOp.GROUP);
        this.schema = schema;
        this.keys = keys;
        this.exprs = exprs;
    }

    public Schema getSchema() {
        return schema;
    }


    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}


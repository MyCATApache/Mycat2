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
package io.mycat.hbt.ast.base;

import io.mycat.hbt.ast.AggregateCall;
import io.mycat.hbt.ast.modify.ModifyTable;
import io.mycat.hbt.ast.query.*;

/**
 * @author jamie12221
 **/
public interface NodeVisitor {
    void visit(MapSchema mapSchema);

    void visit(GroupSchema groupSchema);

    void visit(LimitSchema limitSchema);

    void visit(FromSchema fromSchema);

    void visit(SetOpSchema setOpSchema);

    void visit(FieldType fieldSchema);

    void visit(Literal literal);

    void visit(OrderSchema orderSchema);

    void visit(Identifier identifier);

    void visit(Expr expr);

    void visit(ValuesSchema valuesSchema);

    void visit(JoinSchema corJoinSchema);

    void visit(AggregateCall aggregateCall);

    void visit(FilterSchema filterSchema);

    void visit(ModifyTable modifyTable);

    void visit(DistinctSchema distinctSchema);

    void visit(RenameSchema projectSchema);

    void visit(CorrelateSchema correlate);

}
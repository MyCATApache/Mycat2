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

import io.mycat.hbt.ast.HBTOp;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

/**
 * @author jamie12221
 **/
@Getter
@EqualsAndHashCode
public class AggregateCall extends Node {
    private final String function;
    private final String alias; // may be null
    private final List<Expr> operands; // may be empty, never null
    private final Boolean distinct;
    private final Boolean approximate;
    private final Boolean ignoreNulls;
    private final Expr filter; // may be null
    private final List<OrderItem> orderKeys; // may be empty, never null

    public AggregateCall(String function,List<Expr> operands) {
       this(function,null,operands,null,null,null,null,null);
    }
    public AggregateCall(String function, String alias, List<Expr> operands, Boolean distinct, Boolean approximate, Boolean ignoreNulls, Expr filter, List<OrderItem> orderKeys) {
        super(HBTOp.AGGREGATE_CALL);
        this.function = function;
        this.distinct = distinct;
        this.approximate = approximate;
        this.ignoreNulls = ignoreNulls;
        this.filter = filter;
        this.alias = alias;
        this.operands = operands;
        this.orderKeys = orderKeys;
    }

    //
    public AggregateCall filter(Expr condition) {
        return new AggregateCall(function, alias, operands, distinct, approximate, ignoreNulls, condition, orderKeys);
    }

    /**
     * Returns a copy of this AggCall that sorts its input values by
     * {@code orderKeys} before aggregating, as in SQL's {@code WITHIN GROUP}
     * clause.
     */
    public AggregateCall orderBy(List<OrderItem> orderKeys) {
        return new AggregateCall(function, alias, operands, distinct, approximate, ignoreNulls, filter, orderKeys);
    }

    /**
     * Returns a copy of this AggCall that may return approximate results
     * if {@code approximate} is true.
     */
    public AggregateCall approximate(boolean approximate) {
        return new AggregateCall(function, alias, operands, distinct, approximate, ignoreNulls, filter, orderKeys);
    }
    public AggregateCall approximate() {
        return approximate(true);
    }
    /**
     * Returns a copy of this AggCall that ignores nulls.
     */
    public AggregateCall ignoreNulls(boolean ignoreNulls) {
        return new AggregateCall(function, alias, operands, distinct, approximate, ignoreNulls, filter, orderKeys);
    }
    public AggregateCall ignoreNulls() {
        return ignoreNulls(true);
    }
    /**
     * Returns a copy of this AggCall with a given alias.
     */
    public AggregateCall alias(String alias) {
        return new AggregateCall(function, alias, operands, distinct, approximate, ignoreNulls, filter, orderKeys);
    }

    /**
     * Returns a copy of this AggCall that is optionally distinct.
     */
    public AggregateCall distinct(boolean distinct) {
        return new AggregateCall(function, alias, operands, distinct, approximate, ignoreNulls, filter, orderKeys);
    }

    /**
     * Returns a copy of this AggCall that is distinct.
     */
    public AggregateCall distinct() {
        return distinct(true);
    }

    public AggregateCall all() {
        return distinct(false);
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("AggregateCall(");
        if (function != null) {
            stringBuilder.append("function='").append(function).append('\'');
        }
        if (distinct != null) {
            stringBuilder.append(", ").append("distinct=").append(distinct);
        }
        if (approximate != null) {
            stringBuilder.append(", ").append("approximate=").append(approximate).append(", ");
        }
        if (ignoreNulls != null) {
            stringBuilder.append(", ").append("ignoreNulls=").append(ignoreNulls);
        }
        if (filter != null) {
            stringBuilder.append(", ").append("filter=").append(filter);
        }
        if (alias != null) {
            stringBuilder.append(", ").append("alias='").append(alias).append('\'');
        }
        if (operands != null) {
            stringBuilder.append(", ").append("operands=").append(operands);
        }
        if (orderKeys != null) {
            stringBuilder.append(", ").append("orderKeys=").append(orderKeys).append(')');
        }
        return stringBuilder.toString();
    }
}
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
package io.mycat.hbt;

/**
 * @author jamie12221
 **/
public enum Op {
    //SET OPERATORS
    UNION_DISTINCT("unionDistinct"),
    UNION_ALL("unionAll"),
    EXCEPT_DISTINCT("exceptDistinct"),
    EXCEPT_ALL("exceptAll"),
    MINUS_ALL("minusAll"),
    MINUS_DISTINCT("minusDistinct"),
    INTERSECT_DISTINCT("intersectDistinct"),
    INTERSECT_ALL("intersectAll"),
//    ORDER_ITEM("orderItem"),
    //relational operators
    FROM("from"),
    MAP("map"),
    FILTER("filter"),
    LIMIT("limit"),
    ORDER("orderBy"),
    GROUP("groupBy"),
    TABLE("table"),
    DISTINCT("distinct"),
    RENAME("reName"),
    INNER_JOIN("innerJoin"),
    LEFT_JOIN("leftJoin"),
    CORRELATE_INNER_JOIN("correlateInnerJoin"),
    CORRELATE_LEFT_JOIN("correlateLeftJoin"),
    RIGHT_JOIN("rightJoin"),
    FULL_JOIN("fillJoin"),
    SEMI_JOIN("semiJoin"),
    ANTI_JOIN("antiJoin"),
//    CORRELATE("correlate"),

    // types
    SCHEMA("schema"),
    FIELD_SCHEMA("fieldSchema"),

    //atoms
    LITERAL("literal"),
    IDENTIFIER("id"),
    PROPERTY("property"),

    //debug
    DESCRIBE("describe"),
    DUMP("dump"),

    // operators
    DOT("dot"),
    EQ("eq"),
    NE("ne"),
    GT("gt"),
    LT("lt"),
    GTE("gte"),
    LTE("lte"),
    PLUS("plus"),
    MINUS("minus"),
    AND("and"),
    OR("or"),
    NOT("not"),
    AS_COLUMNNAME("asColumnName"),
    CAST("cast"),
    FUN("fun"),
    REF("ref"),
    AggregateCall("aggregateCall"),
    REGULAR("regular"),
    ;

    String fun;

    Op(String fun) {
        this.fun = fun;
    }

    public String getFun() {
        return fun;
    }
}
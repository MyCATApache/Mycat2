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
package io.mycat.hbt.ast;

/**
 * @author jamie12221
 **/
public enum HBTOp {
    //SET OPERATORS
    UNION_DISTINCT("unionDistinct"),
    UNION_ALL("unionAll"),
    EXCEPT_DISTINCT("exceptDistinct"),
    EXCEPT_ALL("exceptAll"),
//    MINUS_ALL("minusAll"),
//    MINUS_DISTINCT("minusDistinct"),
    INTERSECT_DISTINCT("intersectDistinct"),
    INTERSECT_ALL("intersectAll"),
    //    ORDER_ITEM("orderItem"),
    //relational operators
    FROM_TABLE("fromTable"),
    MERGE_MODIFY("mergeModify"),
    MODIFY_FROM_SQL("modifyFromSql"),
    FROM_SQL("fromSql"),
    FROM_REL_TO_SQL("fromRelToSql"),
    FILTER_FROM_TABLE("filterFromTable"),
    MAP("map"),
    FILTER("filter"),
    LIMIT("limit"),
    ORDER("orderBy"),
    GROUP("groupBy"),
    TABLE("table"),
    DISTINCT("distinct"),
    RENAME("rename"),
    INNER_JOIN("innerJoin"),
    LEFT_JOIN("leftJoin"),
    CORRELATE_INNER_JOIN("correlateInnerJoin"),
    CORRELATE_LEFT_JOIN("correlateLeftJoin"),
    RIGHT_JOIN("rightJoin"),
    FULL_JOIN("fullJoin"),
    SEMI_JOIN("semiJoin"),
    ANTI_JOIN("antiJoin"),
    //    CORRELATE("correlate"),
    // types
    SCHEMA("schema"),
    FIELD_TYPE("fieldType"),

    //atoms
    LITERAL("literal"),
    IDENTIFIER("columnName"),
    PROPERTY("property"),

    //debug
    EXPLAIN("explainHbt"),
    EXPLAIN_SQL("explainSql"),
    // operators
    DOT("dot"),
    EQ("eq"),
    NE("ne"),
    GT("gt"),
    LT("lt"),
    GTE("gte"),
    LTE("lte"),
    ADD("add"),
    MINUS("minus"),
    AND("and"),
    OR("or"),
    NOT("not"),
    AS_COLUMN_NAME("as"),
    CAST("cast"),
    FUN("fun"),
    REF("ref"),
    AGGREGATE_CALL("aggregateCall"),
    REGULAR("regular"),
    PARAM("param"),
    ;

    String fun;

    HBTOp(String fun) {
        this.fun = fun;
    }

    public String getFun() {
        return fun;
    }
}
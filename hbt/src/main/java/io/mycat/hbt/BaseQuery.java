///**
// * Copyright (C) <2020>  <chen junwen>
// * <p>
// * This program is free software: you can redistribute it and/or modify it under the terms of the
// * GNU General Public License as published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// * General Public License for more details.
// * <p>
// * You should have received a copy of the GNU General Public License along with this program.  If
// * not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat.hbt;
//
//import io.mycat.hbt.ast.AggregateCall;
//import io.mycat.hbt.ast.Direction;
//import io.mycat.hbt.ast.base.*;
//import io.mycat.hbt.ast.query.*;
//import org.jetbrains.annotations.NotNull;
//
//import java.io.PrintStream;
//import java.time.LocalDate;
//import java.time.LocalDateTime;
//import java.time.LocalTime;
//import java.util.*;
//import java.util.stream.Collectors;
//
///**
// * @author jamie12221
// **/
//public class BaseQuery {
//    public static Schema empty() {
//        return new ValuesSchema(Collections.emptyList(), Collections.emptyList());
//    }
//
//    public static Schema all(Schema schema) {
//        return schema;
//    }
//
//    public static Schema distinct(Schema schema) {
//        return new DistinctSchema(schema);
//    }
//
//    public static Schema map(Schema table, Expr... id) {
//        return new MapSchema(table, Arrays.asList(id));
//    }
//
//    public static Schema map(Schema table, List<Expr> ids) {
//        return new MapSchema(table, ids);
//    }
//
//    public static Schema map(Schema table, String... id) {
//        return new MapSchema(table, Arrays.asList(id).stream().map(i -> new Identifier(i)).collect(Collectors.toList()));
//    }
//
//    public static List<Literal> values(Object... values) {
//        return Arrays.stream(values).map(i -> literal(i)).collect(Collectors.toList());
//    }
//
//    public static List<Literal> values(Literal... values) {
//        return Arrays.asList(values);
//    }
//
//    public static LocalDateTime timeStamp(String s) {
//        return LocalDateTime.parse(s);
//    }
//
//    public static Literal timeStamp(Literal s) {
//        return literal(timeStamp((String) s.getValue()));
//    }
//
//    public static Literal timeLiteral(String s) {
//        return literal(time(s));
//    }
//
//    public static LocalTime time(String s) {
//        return LocalTime.parse(s);
//    }
//
//    public static Literal time(Literal s) {
//        return literal(LocalTime.parse((String) s.getValue()));
//    }
//
//    public static Literal timeStampLiteral(String s) {
//        return literal(timeStamp(s));
//    }
//
//    public static Literal dateLiteral(String s) {
//        return literal(date(s));
//    }
//
//    public static LocalDate date(String s) {
//        return LocalDate.parse(s);
//    }
//
//    public static Literal date(Literal s) {
//        return literal(date((String) s.getValue()));
//    }
//
//    public static List<FieldType> fields(FieldType... fields) {
//        return Arrays.asList(fields);
//    }
//
//    public static ValuesSchema valuesSchema(List<FieldType> fields, List<Literal> values) {
//        return new ValuesSchema(fields, values);
//    }
//
//    public static List<Node> schemaValues(Node... values) {
//        return Arrays.asList(values);
//    }
//
//    public static Schema limit(Schema from, long offset, long limit) {
//        return new LimitSchema(from, new Literal(offset), new Literal(limit));
//    }
//
//    public static List<Expr> groupKeys(Expr... columnNames) {
//        return list(columnNames);
//    }
//
//    public static Schema exceptAll(Schema from, Schema... from1) {
//        return exceptAll(from, list(from1));
//    }
//
//    public static Schema exceptAll(Schema from, List<Schema> from1) {
//        return new SetOpSchema(Op.EXCEPT_ALL, list(from, from1));
//    }
//
//    public static Schema exceptDistinct(Schema schema, Schema... froms) {
//        return exceptDistinct(schema, list(froms));
//    }
//
//    public static Schema orderBy(Schema from, OrderItem... orderColumns) {
//        return new OrderSchema(from, Arrays.asList(orderColumns));
//    }
//
//    public static Schema orderBy(Schema from, List<OrderItem> orderColumns) {
//        return new OrderSchema(from, orderColumns);
//    }
//
//    public static OrderItem order(String columnName, String direction) {
//        return new OrderItem(new Identifier(columnName), Direction.parse(direction));
//    }
//
//    public static List<OrderItem> orderKeys(OrderItem... items) {
//        return Arrays.asList(items);
//    }
//
//    public static org.apache.calcite.util.Pair<String, Direction> order(String columnName, Direction direction) {
//        return new org.apache.calcite.util.Pair<String, Direction>(columnName, direction);
//    }
//
//    @NotNull
//    public static <T> List<T> list(T schema, T... froms) {
//        ArrayList<T> objects = new ArrayList<>(froms.length + 1);
//        objects.add(schema);
//        objects.addAll(Arrays.asList(froms));
//        return objects;
//    }
//
//    public static Literal literal(Object value) {
//        return new Literal(value);
//    }
//
//
//    public static Expr or(Expr left, Expr right) {
//        return funWithSimpleAlias("or", left, right);
//    }
//
//    public static Identifier id(String value) {
//        return new Identifier(value);
//    }
//
//    public static Identifier id(Identifier value) {
//        return value;
//    }
//
//    public static Identifier id(Literal value) {
//        return new Identifier(Objects.toString(value.getValue()));
//    }
//
//    public static FieldType fieldType(Identifier fieldName, Identifier type) {
//        return new FieldType(fieldName.getValue(), type.getValue());
//    }
//
//    public static FieldType fieldType(String fieldName, String type) {
//        return new FieldType(fieldName, type);
//    }
//
//    public static FromSchema from(Identifier... names) {
//        return from(list(names));
//    }
//
//    public static FromSchema from(List<Identifier> names) {
//        return new FromSchema(names);
//    }
//
//    public static FromSchema from(String... names) {
//        return from(Arrays.stream(names).map(i -> id(i)).collect(Collectors.toList()));
//    }
//
//    public static <T> List<T> list(T... schema) {
//        return Arrays.asList(schema);
//    }
//
//    public static Schema unionDistinct(Schema schema, Schema... froms) {
//        return new SetOpSchema(Op.UNION_DISTINCT, list(schema, froms));
//    }
//
//    public static AggregateCall callWithAlias(String function,  String... columnNames) {
//        return call(function,  Arrays.stream(columnNames).map(i -> id(i)).collect(Collectors.toList()));
//    }
//
//    public static Expr eq(Expr left, Expr right) {
//        return funWithSimpleAlias("eq", left, right);
//    }
//
//
//    public static Expr ne(Expr left, Expr right) {
//        return funWithSimpleAlias("ne", left, right);
//    }
//
//    public static Expr gt(Expr left, Expr right) {
//        return funWithSimpleAlias("gt", left, right);
//    }
//
//    public static Expr gte(Expr left, Expr right) {
//        return funWithSimpleAlias("gte", left, right);
//    }
//
//    public static Expr lt(Expr left, Expr right) {
//        return funWithSimpleAlias("lt", left, right);
//    }
//
//    public static Expr lte(Expr left, Expr right) {
//        return funWithSimpleAlias("lte", left, right);
//    }
//
//    public static Expr and(Expr left, Expr right) {
//        return funWithSimpleAlias("and", left, right);
//    }
//
//    public static Expr or(Expr node, List<Expr> nodes) {
//        if (nodes.isEmpty()) {
//            return node;
//        }
//        int size = nodes.size();
//        if (size == 1) {
//            return or(node, nodes.get(0));
//        }
//        Expr res = node;
//        for (int i = 1; i < size; i++) {
//            res = or(res, nodes.get(i));
//        }
//        return res;
//    }
//
//    public static Expr not(Expr value) {
//        return funWithSimpleAlias("not", value);
//    }
//
//    public static <T> List<T> list(T schema, List<T> froms) {
//        ArrayList<T> objects = new ArrayList<>(froms.size() + 1);
//        objects.add(schema);
//        objects.addAll(froms);
//        return objects;
//    }
//
//    public static <T> List<T> list(List<T> schema, List<T> froms) {
//        ArrayList<T> objects = new ArrayList<>(froms.size() + 1);
//        objects.addAll(schema);
//        objects.addAll(froms);
//        return objects;
//    }
//
//    public static Expr plus(Expr left, Expr right) {
//        return funWithSimpleAlias("plus", left, right);
//    }
//
//    public static Expr minus(Expr left, Expr right) {
//        return funWithSimpleAlias("minus", left, right);
//    }
//
//
//    public static void describe(Schema table) {
//        describe(table, System.out);
//    }
//
//    public static void describe(Schema table, PrintStream out) {
//        out.print('[');
//        for (FieldType field : table.fields()) {
//            out.print(field.getId());
//            out.print(':');
//            out.print(field.getType());
//        }
//        out.print(']');
//    }
//
//    public static Schema filter(Schema asSchema, Expr expr) {
//        return new FilterSchema(asSchema, expr);
//    }
//
//
//    public static Schema projectNamed(Schema schema, List<Identifier> alias) {
//        return new RenameSchema(schema, alias.stream().map(i -> i.getValue()).collect(Collectors.toList()));
//    }
//
//    public static Schema projectNamed(Schema schema, String... alias) {
//        return projectNamed(schema, Arrays.stream(alias).map(i -> id(i)).collect(Collectors.toList()));
//    }
//
//    public static AggregateCall call(String function, String... columnNames) {
//        return call(function, Arrays.stream(columnNames).map(i -> literal(i)).collect(Collectors.toList()));
//    }
//
//    public static AggregateCall call(String function, Expr... columnNames) {
//        return call(function, list(columnNames));
//    }
//
//    public static List<Node> tuple(Node... values) {
//        return Arrays.asList(values);
//    }
//
//
//    public static AggregateCall avg(String columnName) {
//        return callWithSimpleAlias("avg", columnName);
//    }
//
//    public static AggregateCall count() {
//        return callWithSimpleAlias("count");
//    }
//
//    public static AggregateCall count(String columnName) {
//        return callWithSimpleAlias("count", columnName);
//    }
//
//
//
//    public static AggregateCall call(String function, List<Expr> nodes, boolean distinct, boolean approximate, boolean ignoreNulls, Expr filter, List<OrderItem> orderKeys) {
//        return new AggregateCall(function,function, nodes, distinct, approximate, ignoreNulls, filter, orderKeys);
//    }
//
//    public static Schema unionDistinct(Schema schema, List<Schema> froms) {
//        return new SetOpSchema(Op.UNION_DISTINCT, list(schema, froms));
//    }
//
//    public static Schema unionAll(Schema... froms) {
//        return unionAll(froms[0], Arrays.asList(froms).subList(1, froms.length));
//    }
//
//    public static Schema unionAll(Schema from, List<Schema> schemas) {
//        return new SetOpSchema(Op.UNION_ALL, list(from, schemas));
//    }
//
//
//    public static Schema exceptDistinct(Schema schema, List<Schema> froms) {
//        return new SetOpSchema(Op.EXCEPT_DISTINCT, list(schema, froms));
//    }
//
//    public static AggregateCall call(String function, List<Expr> nodes) {
//        return new AggregateCall(function, function, nodes, null, null, null, null, null);
//    }
//
//    public static AggregateCall callWithSimpleAlias(String function, String... columnNames) {
//        return callWithAlias(function, columnNames);
//    }
//
//    public static Schema group(Schema from, List<GroupItem> groupItems) {
//        return group(from, groupItems, Collections.emptyList());
//    }
//
//    public static Schema group(Schema from, List<GroupItem> groupItems, List<AggregateCall> calls) {
//        return new GroupSchema(from, groupItems, calls);
//    }
//
//    public static GroupItem regular(String... nodes) {
//        return regular(Arrays.stream(nodes).map(i -> id(i)).collect(Collectors.toList()));
//    }
//
//    public static Expr funWithSimpleAlias(String fun, Expr... nodes) {
//        return funWithSimpleAlias(fun, list(nodes));
//    }
//
//    public static <T> List<T> keys(T... keys) {
//        return list(keys);
//    }
//
//    public static List<AggregateCall> aggregating(AggregateCall... keys) {
//        return list(keys);
//    }
//
//    public static Expr funWithSimpleAlias(String fun, List<Expr> nodes) {
//        return new Fun(fun, nodes);
//    }
//
//    public static AggregateCall first(String columnName) {
//        return callWithSimpleAlias("first", columnName);
//    }
//
//    public static AggregateCall last(String columnName) {
//        return callWithSimpleAlias("last", columnName);
//    }
//
//    public static AggregateCall max(String columnName) {
//        return callWithSimpleAlias("max", columnName);
//    }
//
//    public static AggregateCall min(String columnName) {
//        return callWithSimpleAlias("min", columnName);
//    }
//
//    public static AggregateCall sum(String columnName) {
//        return callWithSimpleAlias("sum", columnName);
//    }
//
//    public static Expr between(String column, Object start, Object end) {
//        return between(new Identifier(column), literal(start), literal(end));
//    }
//
//    public static GroupItem regular(Expr... nodes) {
//        return regular(list(nodes));
//    }
//    public static Expr in(Identifier column, Literal one, Literal two) {
//        return in(column.getValue(), one,two);
//    }
//
//    public static Expr in(Identifier column, Literal... values) {
//        return in(column.getValue(), values[0], Arrays.asList(values).subList(1,values.length));
//    }
//
//    public static Expr in(String column, Object... values) {
//        return in(column, literal(values[0]), Arrays.stream(values).map(i -> literal(i)).collect(Collectors.toList()));
//    }
//
//    public static GroupItem regular(List<Expr> nodes) {
//        return new GroupItem(Op.REGULAR, nodes);
//    }
//
//    public static Expr between(Expr column, Expr start, Expr end) {
//        return and(gte(column, start), lte(column, end));
//    }
//
//    public static Expr in(Identifier column, Expr... values) {
//        return in(column, values[0], Arrays.asList(values).subList(1, values.length));
//    }
//
//    public static Expr in(String column, Expr... values) {
//        return in(column, values[0], Arrays.asList(values).subList(1, values.length));
//    }
//
//    public static Expr now() {
//        return funWithSimpleAlias("now", Collections.emptyList());
//    }
//
//    public static Expr format(String... columnNames) {
//        return format(new Identifier(columnNames[0]), new Identifier(columnNames[1]));
//    }
//
//    public static Expr in(String column, Expr value, List<Expr> values) {
//        return in(new Identifier(column), value, values);
//    }
//
//    public static Expr in(Expr column, Expr value, List<Expr> values) {
//        if (values.isEmpty()) {
//            return eq(column, value);
//        } else {
//            return or(eq(column, value), values.stream().map(i -> eq(column, i)).collect(Collectors.toList()));
//        }
//    }
//
//    public static Expr format(Expr... nodes) {
//        return format(list(nodes));
//    }
//
//    public static Expr ucase(String columnName) {
//        return funWithSimpleAlias("ucase", columnName);
//    }
//
//    public static Expr upper(String columnName) {
//        return funWithSimpleAlias("upper", columnName);
//    }
//
//    public static Expr lcase(String columnName) {
//        return funWithSimpleAlias("lcase", columnName);
//    }
//
//    public static Expr lower(String columnName) {
//        return funWithSimpleAlias("lower", columnName);
//    }
//
//    public static Expr mid(String columnName, long start, long limit) {
//        return mid(new Identifier(columnName), new Literal(start), new Literal(limit));
//    }
//
//    public static Expr mid(String columnName, long start) {
//        return mid(new Identifier(columnName), new Literal(start));
//    }
//
//    public static Expr format(Expr node, String format) {
//        return format(node, new Literal(format));
//    }
//
//    public static Expr len(String columnName) {
//        return len(new Identifier(columnName));
//    }
//
//    public static Expr format(List<Expr> nodes) {
//        return funWithSimpleAlias("format", nodes);
//    }
//
//    public static Expr round(String column, int decimals) {
//        return round(new Identifier(column), new Literal(decimals));
//    }
//
//    public static Expr mid(Expr... start) {
//        return funWithSimpleAlias("mid", start);
//    }
//
//    public static Expr funWithSimpleAlias(String fun, String... columnNames) {
//        return fun(fun,  columnNames);
//    }
//
//    public static Expr fun(String fun, String... nodes) {
//        return fun(fun, Arrays.stream(nodes).map(i -> id(i)).collect(Collectors.toList()));
//    }
//
//    public static Expr len(Expr... column) {
//        return funWithSimpleAlias("len", column);
//    }
//
//    public static Expr round(Expr... column) {
//        return funWithSimpleAlias("round", column);
//    }
//
//    public static Expr fun(String fun, List<Expr> nodes) {
//        return new Fun(fun, nodes);
//    }
//
//    public AggregateCall countDistinct(String columnName) {
//        return callWithAlias("countDistinct", "count(distinct " + columnName + ")", columnName);
//    }
//
//    public static Schema leftJoin(Expr expr, Schema... froms) {
//        return leftJoin(expr, list(froms));
//    }
//
//    public static Schema leftJoin(Expr expr, List<Schema> froms) {
//        return join(Op.LEFT_JOIN, expr, froms);
//    }
//
//
//    public static Schema rightJoin(Expr expr, Schema... froms) {
//        return rightJoin(expr, list(froms));
//    }
//
//    public static Schema rightJoin(Expr expr, List<Schema> froms) {
//        return join(Op.RIGHT_JOIN, expr, froms);
//    }
//
//    public static Schema fullJoin(Expr expr, Schema... froms) {
//        return fullJoin(expr, list(froms));
//    }
//
//    public static Schema fullJoin(Expr expr, List<Schema> froms) {
//        return join(Op.FULL_JOIN, expr, froms);
//    }
//
//    public static Schema semiJoin(Expr expr, Schema... froms) {
//        return semiJoin(expr, list(froms));
//    }
//
//    public static Schema semiJoin(Expr expr, List<Schema> froms) {
//        return join(Op.SEMI_JOIN, expr, froms);
//    }
//
//    public static Schema antiJoin(Expr expr, Schema... froms) {
//        return antiJoin(expr, list(froms));
//    }
//
//    public static Schema antiJoin(Expr expr, List<Schema> froms) {
//        return join(Op.ANTI_JOIN, expr, froms);
//    }
//
//    public static Schema correlateInnerJoin(Identifier refName, List<Identifier> columnNames, Schema left, Schema right) {
//        return correlate(Op.CORRELATE_INNER_JOIN, refName, columnNames, left, right);
//    }
//
//    public static Schema correlate(Op op, Identifier refName, List<Identifier> columnNames, Schema left, Schema right) {
//        return new CorrelateSchema(op, refName, columnNames, left, right);
//    }
//
//    public static Expr ref(String corName, String fieldName) {
//        return ref(id(corName), id(fieldName));
//    }
//
//    public static Expr ref(Expr corName, Identifier fieldName) {
//        return new Expr(Op.REF, corName, fieldName);
//    }
//
//    public static Schema correlateLeftJoin(Identifier refName, List<Identifier> columnNames, Schema left, Schema right) {
//        return correlate(Op.CORRELATE_LEFT_JOIN, refName, columnNames, left, right);
//    }
//
//
//
//    public static Schema innerJoin(Expr expr, Schema  left,Schema right) {
//        return join(Op.INNER_JOIN, expr, left,right);
//    }
//
//    @NotNull
//    public static Schema join(Op type, Expr expr, Schema  left,Schema right) {
//        return new JoinSchema(type, expr,left,right);
//    }
//
//
//    public static Expr cast(Expr literal, Identifier type) {
//        return new Expr(Op.CAST, literal, type);
//    }
//
//    public static Expr as(Expr literal, String column) {
//        return as(literal, id(column));
//    }
//
//    public static Expr as(Expr literal, Identifier column) {
//        return new Expr(Op.AS_COLUMNNAME, literal, column);
//    }
//
//    public static Expr isnull(String columnName) {
//        return isnull(new Identifier(columnName));
//    }
//
//    public static Expr isnull(Expr columnName) {
//        return funWithSimpleAlias("isnull", columnName);
//    }
//
//    public static Expr ifnull(String columnName, Object value) {
//        return ifnull(new Identifier(columnName), literal(value));
//    }
//
//    public static Expr ifnull(Expr columnName, Expr value) {
//        return funWithSimpleAlias("ifnull", columnName, value);
//    }
//
//    public static Expr isnotnull(String columnName) {
//        return isnotnull(id(columnName));
//    }
//
//    public static Expr isnotnull(Expr columnName) {
//        return funWithSimpleAlias("isnotnull", columnName);
//    }
//
//    public static Expr nullif(String columnName, Object value) {
//        return nullif(id(columnName), literal(value));
//    }
//
//    public static Expr nullif(Expr columnName, Expr value) {
//        return funWithSimpleAlias("nullif", columnName, value);
//    }
//
//    public static AggregateCall distinct(AggregateCall aggregateCall) {
//        return aggregateCall.distinct();
//    }
//
//    public static AggregateCall all(AggregateCall aggregateCall) {
//        return aggregateCall.all();
//    }
//
//    public static AggregateCall approximate(AggregateCall aggregateCall) {
//        return aggregateCall.approximate(true);
//    }
//
//    public static AggregateCall exact(AggregateCall aggregateCall) {
//        return aggregateCall.approximate(false);
//    }
//
//
//    public static AggregateCall filter(AggregateCall aggregateCall, Expr node) {
//        return aggregateCall.filter(node);
//    }
//
//    public static AggregateCall orderBy(AggregateCall aggregateCall, OrderItem... orderKeys) {
//        return orderBy(aggregateCall, list(orderKeys));
//    }
//
//    public static AggregateCall orderBy(AggregateCall aggregateCall, List<OrderItem> orderKeys) {
//        return aggregateCall.orderBy(orderKeys);
//    }
//
//    public static AggregateCall checkNulls(AggregateCall aggregateCall) {
//        return aggregateCall.ignoreNulls(false);
//    }
//
//    public static AggregateCall ignoreNulls(AggregateCall aggregateCall) {
//        return aggregateCall.ignoreNulls(true);
//    }
//
////    public void run() {
////
//////        Schema table = from("db1", "table");
//////        table = as(table, fieldType("id", "string"));
//////        describe(table);
//////        table = filter(table, eq(id("id"), literal(1)));
//////        table = map(table, as(plus(id("id"), literal(1)), "id"));
//////        Values values = values(plus(literal(1), literal(1)), literal(1),
//////                literal(1), literal(2), literal(3));
//////        Schema as = as(values, fieldType("id1", "string"), fieldType("id2", "string"), fieldType("id3", "string"));
////
////    }
//
//
//}
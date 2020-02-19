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

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import io.mycat.hbt.ast.AggregateCall;
import io.mycat.hbt.ast.Direction;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.query.*;
import io.mycat.rsqlBuilder.DesBuilder;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.builder;
import static io.mycat.hbt.Op.*;

/**
 * @author jamie12221
 **/
public class QueryOp {
    private final DesBuilder relBuilder;
    private final Map<String, RexCorrelVariable> correlVariableMap = new HashMap<>();
    public static final HashBiMap<String, SqlAggFunction> sqlAggFunctionMap;
    public static final HashBiMap<String, SqlOperator> sqlOperatorMap;
    public static final HashBiMap<String, SqlTypeName> typeMap;
    public static final HashBiMap<String, Class> type2ClassMap;
    public static final HashBiMap<SqlTypeName, Class> sqlType2ClassMap;

    static {
        typeMap = HashBiMap.create();
        type2ClassMap = HashBiMap.create();
        sqlType2ClassMap = HashBiMap.create();
        sqlAggFunctionMap = HashBiMap.create();
        sqlOperatorMap = HashBiMap.create();

        sqlAggFunctionMap.put("avg", SqlStdOperatorTable.AVG);
        sqlAggFunctionMap.put("count", SqlStdOperatorTable.COUNT);
        sqlAggFunctionMap.put("first", SqlStdOperatorTable.FIRST_VALUE);
        sqlAggFunctionMap.put("last", SqlStdOperatorTable.LAST_VALUE);
        sqlAggFunctionMap.put("max", SqlStdOperatorTable.MAX);
        sqlAggFunctionMap.put("min", SqlStdOperatorTable.MIN);


        sqlOperatorMap.put("eq", SqlStdOperatorTable.EQUALS);
        sqlOperatorMap.put("ne", SqlStdOperatorTable.NOT_EQUALS);
        sqlOperatorMap.put("gt", SqlStdOperatorTable.GREATER_THAN);
        sqlOperatorMap.put("gte", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
        sqlOperatorMap.put("lt", SqlStdOperatorTable.LESS_THAN);
        sqlOperatorMap.put("lte", SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
        sqlOperatorMap.put("and", SqlStdOperatorTable.AND);
        sqlOperatorMap.put("or", SqlStdOperatorTable.OR);
        sqlOperatorMap.put("not", SqlStdOperatorTable.NOT);
        sqlOperatorMap.put("plus", SqlStdOperatorTable.PLUS);
        sqlOperatorMap.put("minus", SqlStdOperatorTable.MINUS);
        sqlOperatorMap.put("dot", SqlStdOperatorTable.DOT);

        sqlOperatorMap.put("lower", SqlStdOperatorTable.LOWER);

        sqlOperatorMap.put("upper", SqlStdOperatorTable.UPPER);

        sqlOperatorMap.put("round", SqlStdOperatorTable.ROUND);

        sqlOperatorMap.put("isnull", SqlStdOperatorTable.IS_NULL);

        sqlOperatorMap.put("nullif", SqlStdOperatorTable.NULLIF);
        sqlOperatorMap.put("isnotnull", SqlStdOperatorTable.IS_NOT_NULL);
        sqlOperatorMap.put("cast", SqlStdOperatorTable.CAST);


        put("boolean", SqlTypeName.BOOLEAN, Boolean.class);

        put("int", SqlTypeName.INTEGER, Integer.class);
        put("float", SqlTypeName.FLOAT, Double.class);
        put("double", SqlTypeName.DOUBLE, BigDecimal.class);
        put("long", SqlTypeName.BIGINT, Long.class);
        put("date", SqlTypeName.DATE, LocalDate.class);
        put("time", SqlTypeName.TIME, LocalTime.class);
        put("timestamp", SqlTypeName.TIMESTAMP, LocalDateTime.class);
        put("varbinary", SqlTypeName.VARBINARY, byte[].class);
        put("varchar", SqlTypeName.VARCHAR, String.class);

    }

    private static void put(String name, SqlTypeName sqlTypeName, Class clazz) {
        typeMap.put(name, sqlTypeName);
//        type2ClassMap.put(name,clazz);
//        sqlType2ClassMap.put(sqlTypeName,clazz);
    }

    private int joinCount;

    public QueryOp(DesBuilder relBuilder) {
        this.relBuilder = relBuilder;
        this.relBuilder.clear();
    }

    private SqlOperator op(String op) {
        SqlOperator sqlOperator = sqlOperatorMap.get(op);
        if (sqlOperator == null) {
            throw new AssertionError("unknown: " + op);
        }
        return sqlOperator;
    }

    private List<RelNode> handle(List<Schema> inputs) {
        return inputs.stream().map(this::handle).collect(Collectors.toList());
    }

    public RelNode complie(Schema root) {
        return handle(root);
    }

    public RelNode handle(Schema input) {
        relBuilder.clear();
        try {
            switch (input.getOp()) {
                case FROM:
                    return from((FromSchema) input);
                case MAP:
                    return map((MapSchema) input);
                case FILTER:
                    return filter((FilterSchema) input);
                case LIMIT:
                    return limit((LimitSchema) input);
                case ORDER:
                    return order((OrderSchema) input);
                case GROUP:
                    return group((GroupSchema) input);
                case TABLE:
                    return values((ValuesSchema) input);
                case DISTINCT:
                    return distinct((DistinctSchema) input);
                case UNION_ALL:
                case UNION_DISTINCT:
                case EXCEPT_ALL:
                case EXCEPT_DISTINCT:
                    return setSchema((SetOpSchema) input);
                case LEFT_JOIN:
                case RIGHT_JOIN:
                case FULL_JOIN:
                case SEMI_JOIN:
                case ANTI_JOIN:
                case INNER_JOIN:
//                    return join((JoinSchema) input);

                    return correlateJoin((JoinSchema) input);
                case RENAME:
                    return rename((RenameSchema) input);
                case CORRELATE_INNER_JOIN:
                case CORRELATE_LEFT_JOIN:
                    return correlate((CorrelateSchema) input);
                default:
            }
        } finally {
            relBuilder.clear();
        }
        throw new UnsupportedOperationException();
    }

    private RelNode correlate(CorrelateSchema input) {
        RelNode left = handle(input.getLeft());
        Holder<RexCorrelVariable> of = Holder.of(null);
        relBuilder.push(left);
        relBuilder.variable(of);
        correlVariableMap.put(input.getRefName(), of.get());
        RelNode right = handle(input.getRight());
        relBuilder.clear();
        final CorrelationId id = of.get().id;
        final ImmutableBitSet requiredColumns =
                RelOptUtil.correlationColumns(id, right);
        relBuilder.push(left);
        List<RexInputRef> collect = requiredColumns.asList().stream().map(i -> relBuilder.field(i)).collect(Collectors.toList());
        relBuilder.clear();
        relBuilder.push(left);
        relBuilder.push(right);
        return relBuilder.correlate(joinOp(input.getOp()), of.get().id, collect).build();
    }


    private RelNode rename(RenameSchema input) {
        RelNode origin = handle(input.getSchema());
        List<String> fieldNames = new ArrayList<>(origin.getRowType().getFieldNames());
        List<String> alias = input.getColumnNames();
        int size = alias.size();
        for (int i = 0; i <size; i++) {
            fieldNames.set(i,alias.get(i));
        }
        if (alias.isEmpty()) {
            return origin;
        } else {
            relBuilder.push(origin);
            relBuilder.projectNamed(relBuilder.fields(), fieldNames, true);
            return relBuilder.build();
        }
    }

    private RelNode correlateJoin(JoinSchema input) {
        List<Schema> schemas = input.getSchemas();
        joinCount = schemas.size();
        try {
            ArrayList<RelNode> nodes = new ArrayList<>(schemas.size());
            HashSet<String> set = new HashSet<>();
            for (Schema schema : schemas) {

                QueryOp queryOp = new QueryOp(relBuilder);
                RelNode relNode =queryOp.complie(schema);
                List<String> fieldNames = relNode.getRowType().getFieldNames();
                if (!set.addAll(fieldNames)) {
                    throw new UnsupportedOperationException();
                }
                nodes.add(relNode);
            }
            for (RelNode relNode : nodes) {
                relBuilder.push(relNode);
            }
            if (input.getCondition() != null) {
                RexNode rexNode = toRex(input.getCondition());

                Set<CorrelationId> collect = correlVariableMap.values().stream().filter(i -> i instanceof RexCorrelVariable)
                        .map(i -> i.id)
                        .collect(Collectors.toSet());
                return relBuilder.join(joinOp(input.getOp()), rexNode, collect).build();
            } else {
                return relBuilder.join(joinOp(input.getOp())).build();
            }
        } finally {
            joinCount = 0;
        }
    }


    private JoinRelType joinOp(Op op) {
        switch (op) {
            case INNER_JOIN:
                return JoinRelType.INNER;
            case LEFT_JOIN:
                return JoinRelType.LEFT;
            case RIGHT_JOIN:
                return JoinRelType.RIGHT;
            case FULL_JOIN:
                return JoinRelType.FULL;
            case SEMI_JOIN:
                return JoinRelType.SEMI;
            case ANTI_JOIN:
                return JoinRelType.ANTI;
            case CORRELATE_INNER_JOIN:
                return JoinRelType.INNER;
            case CORRELATE_LEFT_JOIN:
                return JoinRelType.LEFT;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private RelNode setSchema(SetOpSchema input) {
        int size = input.getSchemas().size();
        RelBuilder relBuilder = this.relBuilder.pushAll(handle(input.getSchemas()));
        switch (input.getOp()) {
            case UNION_DISTINCT:
                return relBuilder.union(false, size).build();
            case UNION_ALL:
                return relBuilder.union(true, size).build();
            case EXCEPT_DISTINCT:
                return relBuilder.minus(false, size).build();
            case EXCEPT_ALL:
                return relBuilder.minus(true, size).build();
            case INTERSECT_DISTINCT:
                return relBuilder.intersect(false, size).build();
            case INTERSECT_ALL:
                return relBuilder.intersect(true, size).build();
            default:
                throw new UnsupportedOperationException();

        }
    }

    private RelNode group(GroupSchema input) {

         relBuilder.push(handle(input.getSchema()));
        RelBuilder.GroupKey groupKey = groupItemListToRex(input.getKeys());
       return relBuilder .aggregate(groupKey, toAggregateCall(input.getExprs()))
                .build();
    }

    private RelBuilder.GroupKey groupItemListToRex(List<GroupItem> keys) {
        ImmutableList.Builder<ImmutableList<RexNode>> builder = builder();
        for (GroupItem key : keys) {
            List<RexNode> nodes = toRex(key.getExprs());
            builder.add(ImmutableList.copyOf(nodes));
        }
        ImmutableList<ImmutableList<RexNode>> build = builder.build();
        if (build.size() == 1) {
            return relBuilder.groupKey(build.get(0));
        } else {
            return relBuilder.groupKey(build.stream().flatMap(u->u.stream()).collect(Collectors.toList()), build);
        }
    }


    private List<RelBuilder.AggCall> toAggregateCall(List<AggregateCall> exprs) {
        return exprs.stream().map(this::toAggregateCall).collect(Collectors.toList());
    }

    private RelBuilder.AggCall toAggregateCall(AggregateCall expr) {
        return relBuilder.aggregateCall(toSqlAggFunction(expr.getFunction()),
                toRex(expr.getOperands() == null ? Collections.emptyList() : expr.getOperands()))
                .sort(expr.getOrderKeys() == null ? Collections.emptyList() : toSortRex(expr.getOrderKeys()))
                .distinct(expr.getDistinct() == Boolean.TRUE)
                .approximate(expr.getApproximate() == Boolean.TRUE)
                .ignoreNulls(expr.getIgnoreNulls() == Boolean.TRUE)
                .filter(expr.getFilter() == null ? null : toRex(expr.getFilter()));
    }

    private SqlAggFunction toSqlAggFunction(String op) {
        SqlAggFunction sqlAggFunction = sqlAggFunctionMap.get(op);
        if (sqlAggFunction == null) {
            throw new UnsupportedOperationException();
        }
        return sqlAggFunction;
    }

    private RelNode from(FromSchema input) {
        List<String> collect = input.getNames().stream().collect(Collectors.toList());
        RelNode build = relBuilder.scan(collect).as(collect.get(collect.size() - 1)).build();
        return build;
    }

    private RelNode map(MapSchema input) {
        RelNode handle = handle(input.getSchema());
        relBuilder.push(handle);
        List<RexNode> nodes = toRex(input.getExpr());
        relBuilder.push(handle);
        relBuilder.project(nodes);
        return relBuilder.build();
    }

    private RelNode filter(FilterSchema input) {
        return relBuilder.push(handle(input.getSchema())).filter(toRex(input.getExpr())).build();
    }

    private RelNode values(ValuesSchema input) {
        return relBuilder.values2(toType(input.getFieldNames()), input.getValues().stream().toArray(Object[]::new)).build();
    }

    private RelNode distinct(DistinctSchema input) {
        RelNode handle = handle(input.getSchema());
        relBuilder.push(handle);
        relBuilder.distinct();
        return relBuilder.peek();
    }

    private RelNode order(OrderSchema input) {
        return relBuilder.push(handle(input.getSchema())).sort(toSortRex(input.getOrders())).build();
    }

    private List<RexNode> toSortRex(List<OrderItem> orders) {
        final List<RexNode> nodes = new ArrayList<>();
        for (OrderItem field : orders) {
            toSortRex(nodes, field);
        }
        return nodes;
    }

    private RelNode limit(LimitSchema input) {
        relBuilder.push(handle(input.getSchema()));
        Number offset = (Number) input.getOffset();
        Number limit = (Number) input.getLimit();
        relBuilder.limit(offset.intValue(), limit.intValue());
        return relBuilder.build();
    }

    private void toSortRex(List<RexNode> nodes, OrderItem pair) {
        if (pair.getColumnName().equalsIgnoreCase("*")) {
            for (RexNode node : relBuilder.fields()) {
                if (pair.getDirection() == Direction.DESC) {
                    node = relBuilder.desc(node);
                }
                nodes.add(node);
            }
        } else {
            RexNode node = toRex(new Identifier(pair.getColumnName()));
            if (pair.getDirection() == Direction.DESC) {
                node = relBuilder.desc(node);
            }
            nodes.add(node);
        }
    }

    public RexNode toRex(Expr node) {
        switch (node.getOp()) {
            case IDENTIFIER: {
                String value = ((Identifier) node).getValue();
                if (value.startsWith("$")) {
                    String substring = value.substring(1, value.length());
                    if (Character.isDigit(substring.charAt(0))) {
                        return relBuilder.field(Integer.parseInt(value.substring(1)));
                    }
                }
                if (joinCount > 1) {
                    for (int i = 0; i < joinCount; i++) {
                        List<String> fieldNames = relBuilder.peek(i).getRowType().getFieldNames();
                        int indexOf = fieldNames.indexOf(value);
                        if (indexOf > -1) {
                            return relBuilder.field(joinCount, i, indexOf);
                        }
                    }
                    throw new UnsupportedOperationException();
                } else {
                    return relBuilder.field(value);
                }
            }
            case LITERAL: {
                Literal node1 = (Literal) node;
                return relBuilder.literal(node1.getValue());
            }
            default: {
                if (node.op == AS_COLUMNNAME) {
                    return as(node);
                } else if (node.op == REF) {
                    return ref(node);
                } else if (node.op == CAST) {
                    RexNode rexNode = toRex(node.getNodes().get(0));
                    Identifier type = (Identifier) node.getNodes().get(1);
                    return relBuilder.cast(rexNode, toType(type.getValue()).getSqlTypeName());
                } else if (node.op == FUN) {
                    Fun node2 = (Fun) node;
                    if ("as".equals(node2.getFunctionName())){
                        return as(node);
                    }
                    if ("ref".equals(node2.getFunctionName())){
                        return ref(node);
                    }
                    return this.relBuilder.call(op(node2.getFunctionName()), toRex(node.getNodes()));
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        }
    }

    private RexNode ref(Expr node) {
        String tableName = ((Identifier) node.getNodes().get(0)).getValue();
        String fieldName = ((Identifier) node.getNodes().get(1)).getValue();
        RexCorrelVariable relNode = correlVariableMap.getOrDefault(tableName, null);
        return relBuilder.field(relNode, fieldName);
    }

    private RexNode as(Expr node) {
        Identifier id = (Identifier) node.getNodes().get(1);
        return this.relBuilder.alias(toRex(node.getNodes().get(0)), id.getValue());
    }

    private List<RexNode> toRex(List<Expr> operands) {
        final ImmutableList.Builder<RexNode> builder = builder();
        for (Expr operand : operands) {
            builder.add(toRex(operand));
        }
        return builder.build();
    }

    private RelDataType toType(String typeText) {
        final RelDataTypeFactory typeFactory = relBuilder.getTypeFactory();
        try {
            return typeFactory.createSqlType(typeMap.get(typeText));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private RelDataType toType(List<FieldType> fieldSchemaList) {
        final RelDataTypeFactory typeFactory = relBuilder.getTypeFactory();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (FieldType fieldSchema : fieldSchemaList) {
            builder.add(fieldSchema.getId(), toType(fieldSchema.getType()));
        }
        return builder.build();
    }
}
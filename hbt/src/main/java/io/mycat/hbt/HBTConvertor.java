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

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.optimizer.rules.TypeInference;
import com.alibaba.fastsql.support.calcite.CalciteMySqlNodeVisitor;
import com.google.common.collect.ImmutableList;
import io.mycat.calcite.*;
import io.mycat.calcite.logic.MycatLogicTable;
import io.mycat.calcite.metadata.LogicTable;
import io.mycat.hbt.ast.AggregateCall;
import io.mycat.hbt.ast.Direction;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.query.*;
import lombok.SneakyThrows;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.builder;
import static io.mycat.hbt.Op.*;

/**
 * @author jamie12221
 **/
public class HBTConvertor {
    private final MycatRelBuilder relBuilder;
    private final Map<String, RexCorrelVariable> correlVariableMap = new HashMap<>();
    private int joinCount;
    private int paramIndex = 0;
    private final List<Object> params;
    private MycatCalciteDataContext context;
    private final Map<String, RelDataType> targetRelDataType = new HashMap<>();

    public HBTConvertor(MycatCalciteDataContext context) {
        this(Collections.emptyList(), context);
    }

    public HBTConvertor(List<Object> params, MycatCalciteDataContext context) {
        this.relBuilder = MycatRelBuilder.create(context);
        this.params = params;
        this.relBuilder.clear();
        this.context = Objects.requireNonNull(context);
    }

    private SqlOperator op(String op) {
        SqlOperator sqlOperator = HBTCalciteSupport.INSTANCE.getSqlOperator(op);
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
                case FROM_TABLE:
                    return fromTable((FromTableSchema) input);
                case FROM_SQL:
                    return fromSql((FromSqlSchema) input);
                case FROM_REL_TO_SQL: {
                    return fromRelToSqlSchema((FromRelToSqlSchema) input);
                }
                case FILTER_FROM_TABLE: {
                    return filterFromTable((FilterFromTableSchema) input);
                }
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

    public RelNode filterFromTable(FilterFromTableSchema input) {
        List<String> names = input.getNames();
        relBuilder.scan(names);
        TableScan tableScan = (TableScan) relBuilder.peek();
        RelOptTable table = tableScan.getTable();
        relBuilder.as(names.get(names.size() - 1));
        relBuilder.filter(toRex(input.getFilter()));
        Filter build = (Filter) relBuilder.build();
        Bindables.BindableTableScan bindableTableScan = Bindables.BindableTableScan.create(build.getCluster(), table, build.getChildExps(), TableScan.identity(table));
        relBuilder.clear();
        return PushDownLogicTable.toPhyTable(relBuilder, bindableTableScan);
    }

    private RelNode fromRelToSqlSchema(FromRelToSqlSchema input) {
        Schema rel = input.getRel();
        RelNode handle = handle(rel);
        return relBuilder.makeTransientSQLScan(input.getTargetName(), handle);
    }

    @SneakyThrows
    public RelNode fromSql(FromSqlSchema input) {
        String targetName = input.getTargetName();
        String sql = input.getSql();
        List<FieldType> fieldTypes = input.getFieldTypes();
        RelDataType relDataType;
        if (true) {
            MycatCalcitePlanner planner = MycatCalciteContext.INSTANCE.createPlanner(context);
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
            sqlStatement.accept(new TypeInference());
            sqlStatement.accept(new MySqlASTVisitorAdapter() {
                @Override
                public boolean visit(SQLExprTableSource x) {
                    return true;
                }
            });
            SchemaPlus rootSchema = context.getRootSchema();
            CalciteMySqlNodeVisitor calciteMySqlNodeVisitor = new CalciteMySqlNodeVisitor();
            SqlNode parse = planner.parse(sql);
            parse =  parse.accept(new SqlShuttle() {
                @Override
                public SqlNode visit(SqlIdentifier id) {
                    if (id.names.size() == 2) {
                        String schema = id.names.get(0);
                        String table = id.names.get(1);
                        MycatLogicTable logicTable = context.getLogicTable(targetName, schema, table);
                        LogicTable table1 = logicTable.getTable();
                        return new SqlIdentifier(Arrays.asList(table1.getSchemaName(),table1.getTableName()),SqlParserPos.ZERO);
                    }
                    return super.visit(id);
                }
            });
            parse = planner.validate(parse);
            relDataType = planner.convert(parse).getRowType();
        } else {
            relDataType = toType(input.getFieldTypes());
        }
        return relBuilder.makeBySql(targetName, relDataType, sql);
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
        List<String> alias = input.getAlias();
        int size = alias.size();
        for (int i = 0; i < size; i++) {
            fieldNames.set(i, alias.get(i));
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
                HBTConvertor queryOp = new HBTConvertor(params, context);
                RelNode relNode = queryOp.complie(schema);
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
        return relBuilder.aggregate(groupKey, toAggregateCall(input.getExprs()))
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
            return relBuilder.groupKey(build.stream().flatMap(u -> u.stream()).collect(Collectors.toList()), build);
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
        SqlAggFunction sqlAggFunction = HBTCalciteSupport.INSTANCE.getAggFunction(op);
        if (sqlAggFunction == null) {
            throw new UnsupportedOperationException();
        }
        return sqlAggFunction;
    }

    private RelNode fromTable(FromTableSchema input) {
        List<String> collect = new ArrayList<>(input.getNames());
        return relBuilder.scan(collect).as(collect.get(collect.size() - 1)).build();
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
        return relBuilder.values(toType(input.getFieldNames()), input.getValues().toArray()).build();
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

    private RexNode toRex(Expr node) {
        switch (node.getOp()) {
            case IDENTIFIER: {
                String value = ((Identifier) node).getValue();
                if (value.startsWith("$")) {
                    String substring = value.substring(1, value.length());
                    if (Character.isDigit(substring.charAt(0)) && Character.isDigit(substring.charAt(substring.length() - 1))) {
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
                } else if (node.op == PARAM) {
                    return relBuilder.literal(params.get(paramIndex++));
                } else if (node.op == FUN) {
                    Fun node2 = (Fun) node;
                    if ("as".equals(node2.getFunctionName())) {
                        return as(node);
                    }
                    if ("ref".equals(node2.getFunctionName())) {
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
        return typeFactory.createSqlType(HBTCalciteSupport.INSTANCE.getSqlTypeName(typeText));
    }

    private RelDataType toType(String typeText, boolean nullable, Integer precision, Integer scale) {
        final RelDataTypeFactory typeFactory = relBuilder.getTypeFactory();
        SqlTypeName sqlTypeName = HBTCalciteSupport.INSTANCE.getSqlTypeName(typeText);
        RelDataType sqlType = null;
        if (precision != null && scale != null) {
            sqlType = typeFactory.createSqlType(sqlTypeName, precision, scale);
        }
        if (precision != null && scale == null) {
            sqlType = typeFactory.createSqlType(sqlTypeName, precision);
        }
        if (precision == null && scale == null) {
            sqlType = typeFactory.createSqlType(sqlTypeName);
        }
        if (sqlType == null) {
            throw new IllegalArgumentException();
        }

        return typeFactory.createTypeWithNullability(sqlType, nullable);
    }

    private RelDataType toType(List<FieldType> fieldSchemaList) {
        final RelDataTypeFactory typeFactory = relBuilder.getTypeFactory();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (FieldType fieldSchema : fieldSchemaList) {
            boolean nullable = fieldSchema.isNullable();
            Integer precision = fieldSchema.getPrecision();
            Integer scale = fieldSchema.getScale();
            builder.add(fieldSchema.getId(), toType(fieldSchema.getType(), nullable, precision, scale));
        }
        return builder.build();
    }
}
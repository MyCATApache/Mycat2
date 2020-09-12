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
import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.MetaCluster;
import io.mycat.MetaClusterCurrent;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatSqlDialect;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.hbt.ast.HBTOp;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.modify.ModifyFromSql;
import io.mycat.hbt.ast.query.*;
import io.mycat.hbt3.Distribution;
import io.mycat.metadata.MetadataManager;
import io.mycat.replica.ReplicaSelectorRuntime;
import lombok.SneakyThrows;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.builder;
import static io.mycat.hbt.ast.HBTOp.*;

//import io.mycat.calcite.rules.PushDownLogicTableRule;

/**
 * @author jamie12221
 **/

public class HBTQueryConvertor {
    final static Logger log = LoggerFactory.getLogger(HBTQueryConvertor.class);
    private final List<Object> params;
    private final RelBuilder relBuilder;
    private final Map<String, RexCorrelVariable> correlVariableMap = new HashMap<>();
    private int joinCount;
    private int paramIndex = 0;
    private final MetaDataFetcher metaDataFetcher;

    public HBTQueryConvertor(List<Object> params, RelBuilder relBuilder) {
        this.params = params;
        this.relBuilder = relBuilder;
        this.relBuilder.clear();

        metaDataFetcher = (targetName, sql) -> {
            try {
                ReplicaSelectorRuntime selectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);
                targetName = selectorRuntime.getDatasourceNameByReplicaName(targetName, false, null);
                JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                JdbcDataSource jdbcDataSource =jdbcConnectionManager.getDatasourceInfo().get(targetName);
                try (Connection connection1 = jdbcDataSource.getDataSource().getConnection()) {
                    try (Statement statement = connection1.createStatement()) {
                        statement.setMaxRows(0);
                        try (ResultSet resultSet = statement.executeQuery(sql)) {
                            ResultSetMetaData metaData = resultSet.getMetaData();
                            JdbcRowMetaData jdbcRowMetaData = new JdbcRowMetaData(metaData);
                            return FieldTypes.getFieldTypes(jdbcRowMetaData);
                        }
                    }
                } catch (SQLException e) {
                    log.warn("{}", e);
                }
                return null;
            } catch (Throwable e) {
                log.warn("{0}", e);
            }
            return null;
        };
    }

    public interface MetaDataFetcher {
        List<FieldType> query(String targetName, String sql);
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
                    return group((GroupBySchema) input);
                case TABLE:
                    return values((AnonyTableSchema) input);
                case DISTINCT:
                    return distinct((DistinctSchema) input);
                case UNION_ALL:
                case UNION_DISTINCT:
                case EXCEPT_ALL:
                case EXCEPT_DISTINCT:
                case INTERSECT_DISTINCT:
                case INTERSECT_ALL:
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
                case MODIFY_FROM_SQL:
                    return modifyFromSql((ModifyFromSql) input);
                default:
            }
        } finally {
            relBuilder.clear();
        }
        throw new UnsupportedOperationException(input.getOp().getFun());
    }

    private RelNode modifyFromSql(ModifyFromSql input) {
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
        relBuilder.clear();
        MycatLogicTable mycatTable = table.unwrap(MycatLogicTable.class);
        Distribution distribution = mycatTable.computeDataNode(ImmutableList.of(build.getCondition()));
        Iterable<DataNode> dataNodes = distribution.getDataNodes(Collections.emptyList());
        return build.copy(build.getTraitSet(), ImmutableList.of(toPhyTable(mycatTable, dataNodes)));
    }

    private RelNode fromRelToSqlSchema(FromRelToSqlSchema input) {
        Schema rel = input.getRel();
        RelNode handle = handle(rel);
        return makeTransientSQLScan(input.getTargetName(), handle, false);
    }

    @SneakyThrows
    public RelNode fromSql(FromSqlSchema input) {
        String targetName = input.getTargetName();
        String sql = input.getSql();
        List<FieldType> fieldTypes = input.getFieldTypes();
        RelDataType relDataType = null;
        if (fieldTypes == null || fieldTypes.isEmpty()) {
            relDataType = tryGetRelDataTypeByParse(sql);
            if (relDataType == null) {
                List<FieldType> fieldTypeList = metaDataFetcher.query(targetName, sql);
                if (fieldTypeList != null) {
                    relDataType = toType(fieldTypeList);
                }
            }
        } else {
            relDataType = toType(fieldTypes);
        }
        Objects.requireNonNull(relDataType, "无法推导sql结果集类型");
        return makeBySql(relDataType, targetName, sql);
    }

    private RelDataType tryGetRelDataTypeByParse(String sql) {
        try {
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            metadataManager.resolveMetadata(sqlStatement);
            if (sqlStatement instanceof SQLSelectStatement) {
                SQLSelectQueryBlock firstQueryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getFirstQueryBlock();
                final RelDataTypeFactory typeFactory = MycatCalciteSupport.INSTANCE.TypeFactory;
                final RelDataTypeFactory.Builder builder = typeFactory.builder();
                for (SQLSelectItem sqlSelectItem : firstQueryBlock.getSelectList()) {
                    SQLDataType sqlDataType = sqlSelectItem.computeDataType();
                    if (sqlDataType == null) {
                        return null;
                    }
                    SqlTypeName type = HBTCalciteSupport.INSTANCE.getSqlTypeByJdbcValue(sqlDataType.jdbcType());
                    if (type == null) {
                        return null;
                    }
                    builder.add(sqlSelectItem.toString(), type);
                }
                return builder.build();
            }
        } catch (Throwable e) {
            log.warn("", e);
        }
        return null;
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
            relBuilder.clear();
            relBuilder.push(origin);
            relBuilder.rename(fieldNames);
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
                HBTQueryConvertor queryOp = new HBTQueryConvertor(params, relBuilder);
                RelNode relNode = queryOp.complie(schema);
                List<String> fieldNames = relNode.getRowType().getFieldNames();
                if (!set.addAll(fieldNames)) {
                    log.warn("dup fieldNames:" + fieldNames);
                }
                nodes.add(relNode);
            }
            for (RelNode relNode : nodes) {
                relBuilder.push(relNode);
            }
            if (input.getCondition() != null) {
                RexNode rexNode = null;
                try {
                    rexNode = toRex(input.getCondition());
                } catch (Exception e) {
                    e.printStackTrace();
                }
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


    private JoinRelType joinOp(HBTOp op) {
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
        if (size > 2) {
            throw new UnsupportedOperationException("set op size must equals 2");
        }
        List<RelNode> nodeList = handle(input.getSchemas());
        if (nodeList.isEmpty()) {
            throw new IllegalArgumentException();
        }
        RelBuilder relBuilder = this.relBuilder.pushAll(nodeList);
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

    private RelNode group(GroupBySchema input) {
        relBuilder.push(handle(input.getSchema()));
        RelBuilder.GroupKey groupKey = groupItemListToRex(input.getKeys());
        return relBuilder.aggregate(groupKey, toAggregateCall(input.getExprs()))
                .build();
    }

    private RelBuilder.GroupKey groupItemListToRex(List<GroupKey> keys) {
        ImmutableList.Builder<ImmutableList<RexNode>> builder = builder();
        for (GroupKey key : keys) {
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
                .distinct(Boolean.TRUE.equals(expr.getDistinct()))
                .approximate(Boolean.TRUE.equals(expr.getApproximate()))
                .ignoreNulls(Boolean.TRUE.equals(expr.getIgnoreNulls()))
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
        RelNode build = relBuilder.scan(collect).as(collect.get(collect.size() - 1)).build();
        MycatLogicTable mycatLogicTable = build.getTable().unwrap(MycatLogicTable.class);

        //消除逻辑表,变成物理表
        if (mycatLogicTable != null) {
            relBuilder.clear();
            Iterable<DataNode> dataNodes = mycatLogicTable.computeDataNode().getDataNodes();
            return toPhyTable(mycatLogicTable, dataNodes);
        }

        return build;
    }

    private RelNode toPhyTable(MycatLogicTable unwrap, Iterable<DataNode> dataNodes) {
        int count = 0;
        for (DataNode dataNode : dataNodes) {
            MycatPhysicalTable mycatPhysicalTable = new MycatPhysicalTable(unwrap, dataNode);
            LogicalTableScan tableScan = LogicalTableScan.create(relBuilder.getCluster(),
                    RelOptTableImpl.create(relBuilder.getRelOptSchema(),
                            unwrap.getRowType(),
                            mycatPhysicalTable,
                            ImmutableList.of(dataNode.getTargetName(),dataNode.getSchema(), dataNode.getTable())),
                    ImmutableList.of()
            );
            count++;
            relBuilder.push(tableScan);
        }
        return relBuilder.union(true, count).build();
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
        relBuilder.push(handle(input.getSchema()));
        RexNode rexNode = toRex(input.getExpr());
        if (correlVariableMap.isEmpty()) {
            relBuilder.filter(rexNode);
        } else {
            relBuilder.filter(correlVariableMap.values().stream().map(i -> i.id).collect(Collectors.toList()), rexNode);
        }
        return relBuilder.build();
    }

    private RelNode values(AnonyTableSchema input) {
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
        HBTOp op = node.getOp();
        switch (op) {
            case IDENTIFIER: {
                String value = ((Identifier) node).getValue();
                if (value.startsWith("$") && Character.isDigit(value.charAt(value.length() - 1))) {//按照下标引用
                    String substring = value.substring(1);
                    if (joinCount > 1) {
                        if (substring.startsWith("$")) {
                            ImmutableList<RexNode> fields = relBuilder.fields();
                            return relBuilder.field(2, 1, Integer.parseInt(substring.substring(1)));
                        }
                        return relBuilder.field(2, 0, Integer.parseInt(substring));
                    }
                    return relBuilder.field(Integer.parseInt(substring));
                }
                if (joinCount > 1) {
                    try {//按照数据源查找字段
                        for (int i = 0; i < joinCount; i++) {
                            List<String> fieldNames = relBuilder.peek(i).getRowType().getFieldNames();
                            int indexOf = fieldNames.indexOf(value);
                            if (indexOf > -1) {
                                try {
                                    return relBuilder.field(joinCount, i, indexOf);
                                } catch (Exception e) {
                                    log.warn("may be a bug");
                                    log.error("", e);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("may be a bug");
                        log.error("", e);
                    }

                    try {
                        char c = value.charAt(value.length() - 1);
                        //按照join命名规则查找
                        if (c == '0') {
                            return relBuilder.field(2, 1, value);
                        }
                    } catch (Exception e) {
                        log.warn("may be a bug");
                        log.error("", e);
                    }
                    return relBuilder.field(value);
                } else {
                    return relBuilder.field(value);
                }
            }
            case LITERAL: {
                Literal node1 = (Literal) node;
                return relBuilder.literal(node1.getValue());
            }
            default: {
                if (node.op == AS_COLUMN_NAME) {
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
        // throw new UnsupportedOperationException();
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

    public static RelDataType toType(String typeText, boolean nullable, Integer precision, Integer scale) {
        final RelDataTypeFactory typeFactory = MycatCalciteSupport.INSTANCE.TypeFactory;
        SqlTypeName sqlTypeName = HBTCalciteSupport.INSTANCE.getSqlTypeName(typeText);
        RelDataType sqlType = null;
        if (precision != null && scale != null) {
            if (sqlTypeName.allowsPrec() && sqlTypeName.allowsScale()) {
                sqlType = typeFactory.createSqlType(sqlTypeName, precision, scale);
            } else if (sqlTypeName.allowsPrec() && !sqlTypeName.allowsScale()) {
                sqlType = typeFactory.createSqlType(sqlTypeName, precision);
            } else if (sqlTypeName.allowsPrec() && !sqlTypeName.allowsScale()) {
                sqlType = typeFactory.createSqlType(sqlTypeName, precision);
            } else if (!sqlTypeName.allowsPrec() && !sqlTypeName.allowsScale()) {
                sqlType = typeFactory.createSqlType(sqlTypeName);
            } else {
                throw new IllegalArgumentException("sqlTypeName:" + sqlTypeName + " precision:" + precision + " scale:" + scale);
            }
        }
        if (precision != null && scale == null) {
            if (sqlTypeName.allowsPrec()) {
                sqlType = typeFactory.createSqlType(sqlTypeName, precision);
            } else {
                sqlType = typeFactory.createSqlType(sqlTypeName);
            }
        }
        if (precision == null && scale == null) {
            sqlType = typeFactory.createSqlType(sqlTypeName);
        }
        if (sqlType == null) {
            throw new IllegalArgumentException("sqlTypeName:" + sqlTypeName);
        }

        return typeFactory.createTypeWithNullability(sqlType, nullable);
    }

    public static RelDataType toType(List<FieldType> fieldSchemaList) {
        final RelDataTypeFactory typeFactory = MycatCalciteSupport.INSTANCE.TypeFactory;
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (FieldType fieldSchema : fieldSchemaList) {
            boolean nullable = fieldSchema.isNullable();
            Integer precision = fieldSchema.getPrecision();
            Integer scale = fieldSchema.getScale();
            builder.add(fieldSchema.getColumnName(), toType(fieldSchema.getColumnType(), nullable, precision, scale));
        }
        return builder.build();
    }

    public RelNode makeTransientSQLScan(String targetName, RelNode input, boolean forUpdate) {
        RelDataType rowType = input.getRowType();
        return makeBySql(rowType, targetName, MycatCalciteSupport.INSTANCE.convertToSql(input, MycatSqlDialect.DEFAULT, forUpdate).getSql()
        );
    }

    /**
     * Creates a literal (constant expression).
     */
    public static RexNode literal(RelDataType type, Object value, boolean allowCast) {
        final RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
        JavaTypeFactoryImpl typeFactory = MycatCalciteSupport.INSTANCE.TypeFactory;
        RexNode literal;
        if (value == null) {
            literal = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        } else if (value instanceof Boolean) {
            literal = rexBuilder.makeLiteral((Boolean) value);
        } else if (value instanceof BigDecimal) {
            literal = rexBuilder.makeExactLiteral((BigDecimal) value);
        } else if (value instanceof Float || value instanceof Double) {
            literal = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) value).doubleValue()));
        } else if (value instanceof Number) {
            literal = rexBuilder.makeExactLiteral(BigDecimal.valueOf(((Number) value).longValue()));
        } else if (value instanceof String) {
            literal = rexBuilder.makeLiteral((String) value);
        } else if (value instanceof Enum) {
            literal = rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.SYMBOL), false);
        } else if (value instanceof byte[]) {
            literal = rexBuilder.makeBinaryLiteral(new ByteString((byte[]) value));
        } else if (value instanceof LocalDate) {
            LocalDate value1 = (LocalDate) value;
            DateString dateString = new DateString(value1.getYear(), value1.getMonthValue(), value1.getDayOfMonth());
            literal = rexBuilder.makeDateLiteral(dateString);
        } else if (value instanceof LocalTime) {
            LocalTime value1 = (LocalTime) value;
            TimeString timeString = new TimeString(value1.getHour(), value1.getMinute(), value1.getSecond());
            literal = rexBuilder.makeTimeLiteral(timeString, -1);
        } else if (value instanceof LocalDateTime) {
            LocalDateTime value1 = (LocalDateTime) value;
            TimestampString timeString = new TimestampString(value1.getYear(), value1.getMonthValue(), value1.getDayOfMonth(), value1.getHour(), value1.getMinute(), value1.getSecond());
            timeString = timeString.withNanos(value1.getNano());
            literal = rexBuilder.makeTimestampLiteral(timeString, -1);
        } else {
            throw new IllegalArgumentException("cannot convert " + value
                    + " (" + value.getClass() + ") to a constant");
        }
        if (allowCast) {
            return rexBuilder.makeCast(type, literal);
        } else {
            return literal;
        }
    }

    public RelBuilder values(RelDataType rowType, Object... columnValues) {
        int columnCount = rowType.getFieldCount();
        final ImmutableList.Builder<ImmutableList<RexLiteral>> listBuilder =
                ImmutableList.builder();
        final List<RexLiteral> valueList = new ArrayList<>();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        for (int i = 0; i < columnValues.length; i++) {
            RelDataTypeField relDataTypeField = fieldList.get(valueList.size());
            valueList.add((RexLiteral) literal(relDataTypeField.getType(), columnValues[i], false));
            if ((i + 1) % columnCount == 0) {
                listBuilder.add(ImmutableList.copyOf(valueList));
                valueList.clear();
            }
        }
        return relBuilder.values(listBuilder.build(), rowType);
    }

    public RexNode literal(Object value) {
        return literal(null, value, false);
    }


    /**
     * todo for update
     *
     * @param targetName
     * @param relDataType
     * @param sql
     * @return
     */
    public MycatTransientSQLTableScan makeBySql(RelDataType relDataType, String targetName, String sql) {
        return new MycatTransientSQLTableScan(relBuilder.getCluster(), relDataType, targetName, sql);
    }
}
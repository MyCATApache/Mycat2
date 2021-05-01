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
package io.mycat.hbt;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.DataNode;
import io.mycat.TableHandler;
import io.mycat.calcite.logical.MycatViewDataNodeMapping;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.hbt.ast.HBTOp;
import io.mycat.hbt.ast.base.AggregateCall;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.query.*;
import io.mycat.calcite.logical.MycatView;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;

/**
 * @author jamie12221 294712221@qq.com
 **/
public class RelNodeConvertor {
    final static Logger log = LoggerFactory.getLogger(RelNodeConvertor.class);

    public static Expr convertRexNode(RexNode condition) {
        ExprExplain exprExplain = new ExprExplain();
        return exprExplain.getExpr(condition);
    }

    public static Schema convertRelNode(RelNode relNode) {
//
//        List<RelNode> inputs = relNode.getInputs();
//        String relTypeName = relNode.getRelTypeName();
//        String correlVariable = relNode.getCorrelVariable();
//        RelOptTable table = relNode.getTable();
//        Set<CorrelationId> variablesSet = relNode.getVariablesSet();
//        switch (relTypeName) {
//            case "LogicalValues": {
//                return logicValues(relNode);
//            }
//            case "LogicalProject": {
//                return logicProject(relNode);
//            }
//            case "LogicalAggregate": {
//                return logicalAggregate(relNode);
//            }
//            case "LogicalTableScan": {
//                return logicalTableScan(relNode);
//            }
//            case "LogicalIntersect":
//            case "LogicalMinus":
//            case "LogicalUnion": {
//                return logicalSetOp(relNode);
//            }
//            case "LogicalSort": {
//                return logicalSort(relNode);
//            }
//            case "LogicalFilter": {
//                return logicalFilter(relNode);
//            }
//            case "LogicalJoin": {
//                return logicalJoin(relNode);
//            }
//            case "LogicalCorrelate": {
//                return logicalCorrelate(relNode);
//            }
//        }
//        if (relNode instanceof MycatTransientSQLTableScan) {
//            List<FieldType> fields = getFields(relNode);
//            MycatTransientSQLTableScan tableScan = (MycatTransientSQLTableScan) relNode;
//            return new FromSqlSchema(fields, tableScan.getTargetName(), tableScan.getSql());
//        }
//        if (relNode instanceof MycatView) {
//            List<FieldType> fields = getFields(relNode);
//            MycatView tableScan = (MycatView) relNode;
//            //todo
//            SqlNode sqlTemplate = tableScan.getSQLTemplate(false);
//            MycatViewDataNodeMapping mycatViewDataNodeMapping = tableScan.getMycatViewDataNodeMapping();
//            Stream<Map<String, DataNode>> apply = mycatViewDataNodeMapping.apply(Collections.emptyList());
//            ImmutableMultimap<String, SqlString> stringStringImmutableMultimap = tableScan.getMycatViewDataNodeMapping().distribution().getDataNodes();
//            List<Schema> fromSqlSchemas = stringStringImmutableMultimap.entries().stream().map(i -> new FromSqlSchema(fields, i.getKey(), i.getValue().getSql())).collect(Collectors.toList());
//            if (fromSqlSchemas.size() > 1) {
//                return new SetOpSchema(HBTOp.UNION_ALL, fromSqlSchemas);
//            } else {
//                return fromSqlSchemas.get(0);
//            }
//        }
//        if (relNode instanceof LogicalTableScan) {
//            List<FieldType> fields = getFields(relNode);
//            LogicalTableScan tableScan = (LogicalTableScan) relNode;
//            MycatPhysicalTable physicalTable = tableScan.getTable().unwrap(MycatPhysicalTable.class);
//            if (physicalTable != null) {
//                DataNode dataNode = physicalTable.getDataNode();
//                String targetName = dataNode.getTargetName();
//                String sql = "select * from " + dataNode.getTargetSchemaTable();
//                return new FromSqlSchema(fields, targetName, sql);
//            }
//            MycatLogicTable mycatLogicTable = tableScan.getTable().unwrap(MycatLogicTable.class);
//            if (mycatLogicTable != null) {
//                TableHandler tableHandler = mycatLogicTable.logicTable();
//                String schemaName = tableHandler.getSchemaName();
//                String tableName = tableHandler.getTableName();
//                return new FromTableSchema(ImmutableList.of(schemaName, tableName));
//            }
//        }
        throw new UnsupportedOperationException();
    }

    private static List<Schema> convertRelNode(List<RelNode> relNodes) {
        return relNodes.stream().map(i -> convertRelNode(i)).collect(Collectors.toList());
    }

    private static Schema logicalCorrelate(RelNode relNode) {
        LogicalCorrelate relNode1 = (LogicalCorrelate) relNode;
        String correlVariable = relNode1.getCorrelVariable();
        Schema left = convertRelNode(relNode1.getLeft());
        Schema right = convertRelNode(relNode1.getRight());
        return new CorrelateSchema(joinType(relNode1.getJoinType(), true), correlVariable, left, right);
    }

    private static Schema logicalJoin(RelNode relNode) {
        LogicalJoin join = (LogicalJoin) relNode;
        JoinRelType joinType = join.getJoinType();
        RelNode rightRelNode = join.getRight();
        RelNode leftRelNode = join.getLeft();
        RexNode condition = join.getCondition();
        List<String> fieldList = join.getRowType().getFieldNames();
        ExprExplain exprExplain = new ExprExplain(join);
        List<RelNode> inputs = join.getInputs();
        Schema left = convertRelNode(leftRelNode);
        Schema right = convertRelNode(rightRelNode);
        return new JoinSchema(joinType(joinType, false), exprExplain.getExpr(condition), left, right);
    }

    private static Schema getJoinLeftExpr(List<String> leftFieldNames, Schema left, List<String> list) {
        int size = leftFieldNames.size();
        boolean needProject = false;
        for (int i = 0; i < size; i++) {
            if (!leftFieldNames.get(i).equals(list.get(i))) {
                needProject = true;
                break;
            }
        }
        if (needProject) {
            left = new RenameSchema(left, list.subList(0, size));
        }
        return left;
    }

    private static Schema getJoinRightExpr(List<String> leftFieldNames, List<String> rightFieldNames, List<String> list, Schema right) {
        int size = list.size();
        int start = leftFieldNames.size();
        boolean needProject = false;
        for (int i = 0; i < rightFieldNames.size(); i++) {
            if (!rightFieldNames.get(i).equals(list.get(start + i))) {
                needProject = true;
                break;
            }
        }
        if (needProject) right = new RenameSchema(right, list.subList(start, size));
        return right;
    }

    private static HBTOp joinType(JoinRelType joinType, boolean cor) {
        switch (joinType) {
            case INNER:
                return cor ? HBTOp.CORRELATE_INNER_JOIN : HBTOp.INNER_JOIN;
            case LEFT:
                return cor ? HBTOp.CORRELATE_LEFT_JOIN : HBTOp.LEFT_JOIN;
            case RIGHT:
                return HBTOp.RIGHT_JOIN;
            case FULL:
                return HBTOp.FULL_JOIN;
            case SEMI:
                return HBTOp.SEMI_JOIN;
            case ANTI:
                return HBTOp.ANTI_JOIN;
        }
        throw new UnsupportedOperationException();
    }

    private static Schema logicalFilter(RelNode relNode) {
        LogicalFilter relNode1 = (LogicalFilter) relNode;
        RexNode condition = relNode1.getCondition();
        List<String> fieldNames = relNode1.getInput().getRowType().getFieldNames();
        Expr expr = convertRexNode(condition);
        return new FilterSchema(convertRelNode(relNode1.getInput()), expr);
    }

    private static List<Expr> getExprs(List<RexNode> map, Join join) {
        ExprExplain exprExplain = new ExprExplain(join);
        return map.stream().map(i -> exprExplain.getExpr(i)).collect(Collectors.toList());
    }

    private static Schema logicalSort(RelNode relNode) {
        LogicalSort relNode1 = (LogicalSort) relNode;
        return getLimit((RexLiteral) relNode1.fetch, (RexLiteral) relNode1.offset, getOrderBy(convertRelNode(relNode1.getInput()), relNode1));
    }

    private static Schema getOrderBy(Schema input, LogicalSort sort) {
        RelCollation collation = sort.getCollation();
        RelNode inputRel = sort.getInput();
        return new OrderSchema(input, getOrderby(inputRel, collation));
    }

    private static List<OrderItem> getOrderby(RelNode inputRel, RelCollation collation) {
        return collation.getFieldCollations().stream().map(fieldCollation -> {
            RelFieldCollation.Direction direction = fieldCollation.getDirection();
            int fieldIndex = fieldCollation.getFieldIndex();
            return new OrderItem(inputRel.getRowType().getFieldNames().get(fieldIndex), Direction.parse(op(direction)));
        }).collect(Collectors.toList());
    }

    private static String op(RelFieldCollation.Direction direction) {
        return direction == RelFieldCollation.Direction.DESCENDING ? "DESC" : "ASC";
    }

    private static Schema getLimit(RexLiteral rexFetch, RexLiteral rexOffset, Schema input) {
        if (rexFetch != null || rexOffset != null) {
            Number offset = 0L;
            Number count = 0L;

            if (rexFetch != null) {
                count = (Number) rexFetch.getValue();
            }

            if (rexOffset != null) {
                offset = (Number) rexOffset.getValue();
            }
            return new LimitSchema(input, offset, count);
        } else {
            return input;
        }
    }

    private static Schema logicalSetOp(RelNode relNode) {
        SetOp logicalUnion = (SetOp) relNode;
        List<Schema> schema = convertRelNode(logicalUnion.getInputs());
        SqlKind kind = logicalUnion.kind;

        ArrayList<Schema> schemas = new ArrayList<>();
        schemas.add(schema.get(0));
        schemas.addAll(schema.subList(1, schema.size()));
        switch (kind) {
            case UNION:
                return new SetOpSchema(logicalUnion.all ? HBTOp.UNION_ALL : HBTOp.UNION_DISTINCT, schemas);
            case EXCEPT:
                return new SetOpSchema(logicalUnion.all ? HBTOp.EXCEPT_ALL : HBTOp.EXCEPT_DISTINCT, schemas);
            case INTERSECT: {
                return new SetOpSchema(logicalUnion.all ? HBTOp.INTERSECT_ALL : HBTOp.INTERSECT_DISTINCT, schemas);
            }
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static Schema logicalTableScan(RelNode relNode) {
        LogicalTableScan tableScan = (LogicalTableScan) relNode;
        RelOptTable table = tableScan.getTable();
        RelDataType rowType = tableScan.getRowType();
        List<String> inputFieldNames = rowType.getFieldNames();
        List<String> outputFieldNames1 = table.getRowType().getFieldNames();

        List<String> qualifiedName = table.getQualifiedName();

        return new FromTableSchema(new ArrayList<>(qualifiedName));
    }

    private static Schema logicalAggregate(RelNode relNode) {
        LogicalAggregate relNode1 = (LogicalAggregate) relNode;
        Schema schema = convertRelNode(relNode1.getInput());
        Aggregate.Group groupType = relNode1.getGroupType();
        return new GroupBySchema(schema, getGroupItems(relNode1), getAggCallList(relNode1.getInput(), relNode1.getAggCallList()));
    }

    private static List<AggregateCall> getAggCallList(RelNode org, List<org.apache.calcite.rel.core.AggregateCall> aggCallList) {
        return aggCallList.stream().map(i -> getAggCallList(org, i)).collect(Collectors.toList());
    }

    private static AggregateCall getAggCallList(RelNode inputRel, org.apache.calcite.rel.core.AggregateCall call) {
        List<String> fieldNames = inputRel.getRowType().getFieldNames();
        RelDataType type = call.getType();
        String alias = call.getName();
        String ageName = HBTCalciteSupport.INSTANCE.getAggFunctionName(call.getAggregation());
        List<Expr> argList = call.getArgList().stream().map(i -> new Identifier(fieldNames.get(i))).collect(Collectors.toList());
        boolean distinct = call.isDistinct();
        boolean approximate = call.isApproximate();
        boolean ignoreNulls = call.ignoreNulls();
        Expr filter = call.hasFilter() ? new Identifier(inputRel.getRowType().getFieldNames().get(call.filterArg)) : null;
        List<OrderItem> orderby = getOrderby(inputRel, call.getCollation());
        return new AggregateCall(ageName, argList).alias(alias).ignoreNulls(ignoreNulls).approximate(approximate).distinct(distinct).filter(filter).orderBy(orderby);
    }

    private static List<GroupKey> getGroupItems(LogicalAggregate aggregate) {
        List<GroupKey> list = new ArrayList<>();
        List<String> fieldNames = aggregate.getInput().getRowType().getFieldNames();
        final ImmutableList<ImmutableBitSet> groupSets = aggregate.getGroupSets();
        for (ImmutableBitSet set : groupSets) {
            List<Expr> arrayList = new ArrayList<>();
            for (Integer integer : set) {
                arrayList.add(new Identifier(fieldNames.get(integer)));
            }
            list.add(new GroupKey(arrayList));
        }
        return list;
    }

    private static Schema logicProject(RelNode relNode) {
        LogicalProject project = (LogicalProject) relNode;
        Schema schema = convertRelNode(project.getInput());
        List<String> fieldNames = project.getInput().getRowType().getFieldNames();
        List<Expr> expr = getExprs(project.getProjects(), null);
        RelDataType outRowType = project.getRowType();
        List<String> outFieldNames = outRowType.getFieldNames();
        ArrayList<Expr> outExpr = new ArrayList<>();

        List<RelDataTypeField> outputRel = relNode.getRowType().getFieldList();
        for (int i = 0; i < outputRel.size(); i++) {
            Expr expr1 = expr.get(i);
            SqlTypeName outType = outputRel.get(i).getType().getSqlTypeName();
            SqlTypeName inType = project.getProjects().get(i).getType().getSqlTypeName();
            if (!outType.equals(inType)) {
                expr1 = new Expr(HBTOp.CAST, Arrays.asList(expr1, new Identifier(ExprExplain.type(outType))));
            }
            String outName = outputRel.get(i).getName();
            Identifier identifier = new Identifier(outName);
            if (!expr1.equals(identifier)) {
                expr1 = new Expr(HBTOp.AS_COLUMN_NAME, Arrays.asList(expr1, identifier));
            }
            outExpr.add(expr1);
        }
        return new MapSchema(schema, outExpr);
    }

    private static Schema logicValues(RelNode relNode) {
        LogicalValues logicalValues = (LogicalValues) relNode;
        return new AnonyTableSchema(getFields(relNode), getValues(logicalValues));
    }

    private static List<Object> getValues(LogicalValues relNode1) {
        ImmutableList<ImmutableList<RexLiteral>> tuples = relNode1.getTuples();
        if (tuples == null) {
            return Collections.emptyList();
        }
        return tuples.stream().flatMap(Collection::stream).map(rexLiteral -> ExprExplain.unWrapper(rexLiteral)).collect(Collectors.toCollection(ArrayList::new));
    }

    private static List<FieldType> getFields(RelNode relNode) {
        RelDataType rowType = relNode.getRowType();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        ArrayList<FieldType> fieldSchemas = new ArrayList<>(fieldList.size());
        for (RelDataTypeField relDataTypeField : fieldList) {
            String name = relDataTypeField.getName();
            RelDataType type = relDataTypeField.getType();
            SqlTypeName sqlTypeName = type.getSqlTypeName();
            boolean nullable = type.isNullable();
            Integer precision = null;
            Integer scale = null;
            if (sqlTypeName.allowsPrec()) {
                precision = type.getPrecision();
            }
            if (sqlTypeName.allowsScale()) {
                scale = type.getScale();
            }
            fieldSchemas.add(new FieldType(name, ExprExplain.type(sqlTypeName), nullable, precision, scale));
        }
        return fieldSchemas;
    }

    static final class ExprExplain {
        final Join join;

        public ExprExplain() {
            this(null);
        }

        public ExprExplain(Join join) {
            this.join = join;
        }

        public static String op(SqlOperator kind) {
            return HBTCalciteSupport.INSTANCE.getSqlOperatorName(kind);
        }

        public static String type(SqlTypeName type) {
            return HBTCalciteSupport.INSTANCE.getSqlTypeName(type);
        }

        public Expr getExpr(RexNode rexNode) {
            if (rexNode instanceof RexLiteral) {
                RexLiteral rexNode1 = (RexLiteral) rexNode;
                Object o = unWrapper(rexNode1);
                return new Literal(o);
            }
            if (rexNode instanceof RexInputRef) {
                RexInputRef expr = (RexInputRef) rexNode;
                if (join == null) {
                    return new Identifier("$" + (expr.getIndex()));
                } else {
                    int leftCount = this.join.getLeft().getRowType().getFieldCount();
                    int fieldCount = this.join.getRowType().getFieldCount();
                    String pre;
                    int index;
                    if (expr.getIndex() < leftCount) {
                        pre = "$";
                        index = expr.getIndex();
                    } else {
                        pre = "$$";
                        index = expr.getIndex() - leftCount;
                    }
                    return new Identifier(pre + index);
                }
            }
            if (rexNode instanceof RexCall) {
                RexCall expr = (RexCall) rexNode;
                List<Expr> exprList = (expr.getOperands()).stream().map(i -> getExpr(i)).collect(Collectors.toList());
                if (expr.getOperator() == CAST) {
                    ArrayList<Expr> args = new ArrayList<>(exprList.size() + 1);
                    args.addAll(exprList);
                    args.add(new Identifier(type(expr.getType().getSqlTypeName())));
                    return new Expr(HBTOp.CAST, args);
                } else {
                    return new Fun(op(expr.op), exprList);
                }
            }
            if (rexNode instanceof RexFieldAccess) {
                RexFieldAccess rexNode1 = (RexFieldAccess) rexNode;
                if (rexNode1.getReferenceExpr() instanceof RexCorrelVariable) {
                    RexCorrelVariable referenceExpr = (RexCorrelVariable) rexNode1.getReferenceExpr();
                    return new Expr(HBTOp.REF, new Identifier(referenceExpr.id.getName()), new Identifier(rexNode1.getField().getName()));
                }
            }
            return null;
        }

        public static Object unWrapper(RexLiteral rexLiteral) {
            if (rexLiteral.isNull()) {
                return null;
            }
            RelDataType type = rexLiteral.getType();
            SqlTypeName sqlTypeName = type.getSqlTypeName();
            switch (sqlTypeName) {
                case BOOLEAN:
                case SMALLINT:
                case TINYINT:
                case INTEGER:
                case BIGINT:
                case DECIMAL:
                case FLOAT:
                case REAL:
                case DOUBLE:
                    return rexLiteral.getValue();
                case DATE: {
                    Integer valueAs = (Integer) rexLiteral.getValue4();
                    return LocalDate.ofEpochDay(valueAs);
                }
                case TIME: {
                    Integer value = (Integer) rexLiteral.getValue4();
                    return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(value));
                }
                case TIME_WITH_LOCAL_TIME_ZONE:
                    break;
                case TIMESTAMP:
                    String s = rexLiteral.toString();
                    DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
                            .parseCaseInsensitive()
                            .append(ISO_LOCAL_DATE)
                            .appendLiteral(' ')
                            .append(ISO_LOCAL_TIME)
                            .toFormatter();
                    return LocalDateTime.parse(s, dateTimeFormatter);
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    break;
                case INTERVAL_YEAR:
                    break;
                case INTERVAL_YEAR_MONTH:
                    break;
                case INTERVAL_MONTH:
                    break;
                case INTERVAL_DAY:
                    break;
                case INTERVAL_DAY_HOUR:
                    break;
                case INTERVAL_DAY_MINUTE:
                    break;
                case INTERVAL_DAY_SECOND:
                    break;
                case INTERVAL_HOUR:
                    break;
                case INTERVAL_HOUR_MINUTE:
                    break;
                case INTERVAL_HOUR_SECOND:
                    break;
                case INTERVAL_MINUTE:
                    break;
                case INTERVAL_MINUTE_SECOND:
                    break;
                case INTERVAL_SECOND:
                    break;
                case CHAR:
                case VARCHAR:
                    return ((NlsString) rexLiteral.getValue()).getValue();
                case BINARY:
                case VARBINARY:
                    return ((org.apache.calcite.avatica.util.ByteString) rexLiteral.getValue()).getBytes();
                case NULL:
                    return null;
                case ANY:
                    break;
                case SYMBOL:
                    break;
                case MULTISET:
                    break;
                case ARRAY:
                    break;
                case MAP:
                    break;
                case DISTINCT:
                    break;
                case STRUCTURED:
                    break;
                case ROW:
                    break;
                case OTHER:
                    break;
                case CURSOR:
                    break;
                case COLUMN_LIST:
                    break;
                case DYNAMIC_STAR:
                    break;
                case GEOMETRY:
                    break;
            }
            throw new UnsupportedOperationException();
        }

    }

}
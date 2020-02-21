package io.mycat.hbt;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import io.mycat.hbt.ast.Direction;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.query.*;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;

public class QueryOp2 {


    public static List<Schema> getSchema(List<RelNode> relNodes) {
        return relNodes.stream().map(i -> getSchema(i)).collect(Collectors.toList());
    }

    public static Schema getSchema(RelNode relNode) {
        List<RelNode> inputs = relNode.getInputs();
        String relTypeName = relNode.getRelTypeName();
        String correlVariable = relNode.getCorrelVariable();
        RelOptTable table = relNode.getTable();
        Set<CorrelationId> variablesSet = relNode.getVariablesSet();
        switch (relTypeName) {
            case "LogicalValues": {
                return logicValues(relNode);
            }
            case "LogicalProject": {
                return logicProject(relNode);
            }
            case "LogicalAggregate": {
                return logicalAggregate(relNode);
            }
            case "LogicalTableScan": {
                return logicalTableScan(relNode);
            }
            case "LogicalIntersect":
            case "LogicalMinus":
            case "LogicalUnion": {
                return logicalSetOp(relNode);
            }
            case "LogicalSort": {
                return logicalSort(relNode);
            }
            case "LogicalFilter": {
                return logicalFilter(relNode);
            }
            case "LogicalJoin": {
                return logicalJoin(relNode);
            }
            case "LogicalCorrelate": {
                return logicalCorrelate(relNode);
            }
        }
        throw new UnsupportedOperationException();
    }

    public static Schema logicalCorrelate(RelNode relNode) {
        LogicalCorrelate relNode1 = (LogicalCorrelate) relNode;
        String correlVariable = relNode1.getCorrelVariable();
        Schema left = getSchema(relNode1.getLeft());
        Schema right = getSchema(relNode1.getRight());
        return new CorrelateSchema(joinType(relNode1.getJoinType(), true),correlVariable, left, right);
    }

    public static Schema logicalJoin(RelNode relNode) {
        LogicalJoin join = (LogicalJoin) relNode;
        JoinRelType joinType = join.getJoinType();
        RexNode condition = join.getCondition();
        List<String> fieldList = join.getRowType().getFieldNames();
        ExprExplain exprExplain = new ExprExplain(fieldList);
        List<RelNode> inputs = join.getInputs();
        Schema left = getSchema(inputs.get(0));
        Schema right = getSchema(inputs.get(1));
        return new JoinSchema(joinType(joinType, false), exprExplain.getExpr(condition), left, right);
    }

    public static Op joinType(JoinRelType joinType, boolean cor) {
        switch (joinType) {
            case INNER:
                return cor ? Op.CORRELATE_INNER_JOIN : Op.INNER_JOIN;
            case LEFT:
                return cor ? Op.CORRELATE_LEFT_JOIN : Op.LEFT_JOIN;
            case RIGHT:
                return Op.RIGHT_JOIN;
            case FULL:
                return Op.FULL_JOIN;
            case SEMI:
                return Op.SEMI_JOIN;
            case ANTI:
                return Op.ANTI_JOIN;
        }
        throw new UnsupportedOperationException();
    }

    public static Schema logicalFilter(RelNode relNode) {
        LogicalFilter relNode1 = (LogicalFilter) relNode;
        RexNode condition = relNode1.getCondition();
        List<String> fieldNames = relNode1.getInput().getRowType().getFieldNames();
        Expr expr = getExpr(condition, fieldNames);
        return new FilterSchema(getSchema(relNode1.getInput()), expr);
    }

    public static Expr getExpr(RexNode condition, List<String> fieldNames) {
        ExprExplain exprExplain = new ExprExplain(fieldNames);
        return exprExplain.getExpr(condition);
    }

    public static List<Expr> getExprs(List<RexNode> map, List<String> fieldNames) {
        ExprExplain exprExplain = new ExprExplain(fieldNames);
        return map.stream().map(i -> exprExplain.getExpr(i)).collect(Collectors.toList());
    }

    public static Schema logicalSort(RelNode relNode) {
        LogicalSort relNode1 = (LogicalSort) relNode;
        return getLimit((RexLiteral) relNode1.fetch, (RexLiteral) relNode1.offset, getOrderBy(getSchema(relNode1.getInput()), relNode1));
    }

    public static Schema getOrderBy(Schema input, LogicalSort sort) {
        RelCollation collation = sort.getCollation();
        RelNode inputRel = sort.getInput();
        return new OrderSchema(input, getOrderby(inputRel, collation));
    }


    public static List<OrderItem> getOrderby(RelNode inputRel, RelCollation collation) {
        return collation.getFieldCollations().stream().map(fieldCollation -> {
            RelFieldCollation.Direction direction = fieldCollation.getDirection();
            int fieldIndex = fieldCollation.getFieldIndex();
            return new OrderItem(inputRel.getRowType().getFieldNames().get(fieldIndex), Direction.parse(op(direction)));
        }).collect(Collectors.toList());
    }

    public static String op(RelFieldCollation.Direction direction) {
        return direction == RelFieldCollation.Direction.DESCENDING ? "DESC" : "ASC";
    }

    public static Schema getLimit(RexLiteral rexFetch, RexLiteral rexOffset, Schema input) {
        if (rexFetch != null || rexOffset != null) {
            Number offset = 0L;
            Number count = 0L;

            if (rexFetch != null) {
                count = (Number) rexFetch.getValue();
            }

            if (rexOffset != null) {
                offset = (Number) rexOffset.getValueAs(Long.class);
            }
            return new LimitSchema(input, offset, count);
        } else {
            return input;
        }
    }

    public static Schema logicalSetOp(RelNode relNode) {
        SetOp logicalUnion = (SetOp) relNode;
        List<Schema> schema = getSchema(logicalUnion.getInputs());
        SqlKind kind = logicalUnion.kind;

        ArrayList<Schema> schemas = new ArrayList<>();
        schemas.add(schema.get(0));
        schemas.addAll(schema.subList(1, schema.size()));
        switch (kind) {
            case UNION:
                return new SetOpSchema(logicalUnion.all ? Op.UNION_ALL : Op.UNION_DISTINCT, schemas);
            case EXCEPT:
                return new SetOpSchema(logicalUnion.all ? Op.EXCEPT_ALL : Op.EXCEPT_DISTINCT, schemas);
            case INTERSECT: {
                return new SetOpSchema(logicalUnion.all ? Op.INTERSECT_ALL : Op.INTERSECT_DISTINCT, schemas);
            }
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static Schema logicalTableScan(RelNode relNode) {
        LogicalTableScan tableScan = (LogicalTableScan) relNode;
        RelOptTable table = tableScan.getTable();
        RelDataType rowType = tableScan.getRowType();
        List<String> inputFieldNames = rowType.getFieldNames();
        List<String> outputFieldNames1 = table.getRowType().getFieldNames();

        List<String> qualifiedName = table.getQualifiedName();

        return new FromSchema(new ArrayList<>(qualifiedName));
    }

    public static Schema logicalAggregate(RelNode relNode) {
        LogicalAggregate relNode1 = (LogicalAggregate) relNode;
        Schema schema = getSchema(relNode1.getInput());
        Aggregate.Group groupType = relNode1.getGroupType();
        return new GroupSchema(schema, getGroupItems(relNode1), getAggCallList(relNode1.getInput(), relNode1.getAggCallList()));
    }

    public static List<io.mycat.hbt.ast.AggregateCall> getAggCallList(RelNode org, List<org.apache.calcite.rel.core.AggregateCall> aggCallList) {
        return aggCallList.stream().map(i -> getAggCallList(org, i)).collect(Collectors.toList());
    }

    public static io.mycat.hbt.ast.AggregateCall getAggCallList(RelNode inputRel, org.apache.calcite.rel.core.AggregateCall call) {
        List<String> fieldNames = inputRel.getRowType().getFieldNames();
        BiMap<SqlAggFunction, String> inverse = QueryOp.sqlAggFunctionMap.inverse();
        RelDataType type = call.getType();
        String alias = call.getName();
        String aggeName = inverse.get(call.getAggregation());
        List<Expr> argList = call.getArgList().stream().map(i -> new Identifier(fieldNames.get(i))).collect(Collectors.toList());
        boolean distinct = call.isDistinct();
        boolean approximate = call.isApproximate();
        boolean ignoreNulls = call.ignoreNulls();
        Expr filter = call.hasFilter() ? new Identifier(inputRel.getRowType().getFieldNames().get(call.filterArg)) : null;
        List<OrderItem> orderby = getOrderby(inputRel, call.getCollation());
        return new io.mycat.hbt.ast.AggregateCall(aggeName, argList).alias(alias).ignoreNulls(ignoreNulls).approximate(approximate).distinct(distinct).filter(filter).orderBy(orderby);
    }

    public static List<GroupItem> getGroupItems(LogicalAggregate aggregate) {
        List<GroupItem> list = new ArrayList<>();
        List<String> fieldNames = aggregate.getInput().getRowType().getFieldNames();
        final ImmutableList<ImmutableBitSet> groupSets = aggregate.getGroupSets();
        for (ImmutableBitSet set : groupSets) {
            List<Expr> arrayList = new ArrayList<>();
            for (Integer integer : set) {
                arrayList.add(new Identifier(fieldNames.get(integer)));
            }
            list.add(new GroupItem(arrayList));
        }
        return list;
    }

    public static Schema logicProject(RelNode relNode) {
        LogicalProject project = (LogicalProject) relNode;
        Schema schema = getSchema(project.getInput());
        List<String> fieldNames = project.getInput().getRowType().getFieldNames();
        List<Expr> expr = getExprs(project.getChildExps(), fieldNames);
        RelDataType outRowType = project.getRowType();
        List<String> outFieldNames = outRowType.getFieldNames();
        int size = outFieldNames.size();
        ArrayList<Expr> outExpr = new ArrayList<>();

        List<RelDataTypeField> outputRel = relNode.getRowType().getFieldList();
        List<RelDataTypeField> inputRel = project.getInput().getRowType().getFieldList();

        for (int i = 0; i < size; i++) {
            Expr expr1 = expr.get(i);

            SqlTypeName outType = outputRel.get(i).getType().getSqlTypeName();
            SqlTypeName inType = inputRel.get(i).getType().getSqlTypeName();
            if(!outType.equals(inType)){
                expr1 = new Expr(Op.CAST, Arrays.asList(expr1, new Identifier(ExprExplain.type(outType))));
            }
            String outName = outputRel.get(i).getName();
            String inName = inputRel.get(i).getName();

            if ((outName != null) & !Objects.equals(outName, (inName))){
                expr1 = new Expr(Op.AS_COLUMNNAME, Arrays.asList(expr1, new Identifier(outName)));
            }
            outExpr.add(expr1);
        }
        return new MapSchema(schema, outExpr);
    }

    public static Schema logicValues(RelNode relNode) {
        LogicalValues logicalValues = (LogicalValues) relNode;
        return new ValuesSchema(getFieldSchema(relNode), getValues(logicalValues));
    }

    public static List<Object> getValues(LogicalValues relNode1) {
        ImmutableList<ImmutableList<RexLiteral>> tuples = relNode1.getTuples();
        if (tuples == null) {
            return Collections.emptyList();
        }
        return tuples.stream().flatMap(Collection::stream).map(rexLiteral -> ExprExplain.unWrapper(rexLiteral)).collect(Collectors.toCollection(ArrayList::new));
    }


    public static List<FieldType> getFieldSchema(RelNode relNode) {
        RelDataType rowType = relNode.getRowType();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        ArrayList<FieldType> fieldSchemas = new ArrayList<>(fieldList.size());
        for (RelDataTypeField relDataTypeField : fieldList) {
            String name = relDataTypeField.getName();
            SqlTypeName sqlTypeName = relDataTypeField.getType().getSqlTypeName();
            fieldSchemas.add(new FieldType(name, ExprExplain.type(sqlTypeName)));
        }
        return fieldSchemas;
    }

    static class ExprExplain {
        final List<String> fieldNames;

        public ExprExplain(List<String> fieldNames) {
            this.fieldNames = fieldNames;
        }

        public static String op(SqlOperator kind) {
            return QueryOp.sqlOperatorMap.inverse().get(kind);
        }

        public static String type(SqlTypeName type) {
            return QueryOp.typeMap.inverse().get(type);
        }

        public Expr getExpr(RexNode rexNode) {
            if (rexNode instanceof RexLiteral) {
                RexLiteral rexNode1 = (RexLiteral) rexNode;
                return new Literal(unWrapper(rexNode1));
            }
            if (rexNode instanceof RexInputRef) {
                RexInputRef expr = (RexInputRef) rexNode;
                return new Identifier(fieldNames.get(expr.getIndex()));
            }
            if (rexNode instanceof RexCall) {
                RexCall expr = (RexCall) rexNode;
                List<Expr> exprList = (expr.getOperands()).stream().map(i -> getExpr(i)).collect(Collectors.toList());
                if (expr.getOperator() == CAST) {
                    ArrayList<Expr> args = new ArrayList<>(exprList.size() + 1);
                    args.addAll(exprList);
                    args.add(new Identifier(type(expr.getType().getSqlTypeName())));
                    return new Expr(Op.CAST, args);
                } else {
                    return new Fun(op(expr.op), exprList);
                }
            }
            if (rexNode instanceof RexFieldAccess) {
                RexFieldAccess rexNode1 = (RexFieldAccess) rexNode;
                if (rexNode1.getReferenceExpr() instanceof RexCorrelVariable) {
                    RexCorrelVariable referenceExpr = (RexCorrelVariable) rexNode1.getReferenceExpr();
                    return new Expr(Op.REF, new Identifier(referenceExpr.id.getName()), new Identifier(rexNode1.getField().getName()));
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
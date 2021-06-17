package io.mycat.calcite.rewriter;

import com.google.common.collect.ImmutableList;
import io.mycat.Partition;
import io.mycat.RangeVariable;
import io.mycat.RangeVariableType;
import io.mycat.querycondition.ComparisonOperator;
import io.mycat.querycondition.QueryType;
import io.mycat.router.CustomRuleFunction;
import io.mycat.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
@ToString
public class ValueIndexCondition implements Comparable<ValueIndexCondition>, Serializable {
    List<String> fieldNames;
    String indexName;
    String indexColumnNames;

    QueryType queryType;
    ComparisonOperator rangeQueryLowerOp;
    List<Object> rangeQueryLowerKey;
    ComparisonOperator rangeQueryUpperOp;
    List<Object> rangeQueryUpperKey;
    List<Object> pointQueryKey;

    public String toJson() {
        Map<String, String> map = new HashMap<>();
        map.put("fieldNames", JsonUtil.toJson(fieldNames));
        map.put("indexName", JsonUtil.toJson(indexName));
        map.put("indexColumnNames", JsonUtil.toJson(indexColumnNames));
        map.put("queryType", JsonUtil.toJson(queryType));
        map.put("rangeQueryLowerOp", JsonUtil.toJson(rangeQueryLowerOp));
        map.put("rangeQueryUpperOp", JsonUtil.toJson(rangeQueryUpperOp));
        map.put("rangeQueryLowerKey", JsonUtil.toJson(rangeQueryLowerKey));
        map.put("rangeQueryUpperKey", JsonUtil.toJson(rangeQueryUpperKey));
        map.put("pointQueryKey", JsonUtil.toJson(pointQueryKey));
        return JsonUtil.toJson(map);
    }

    public static ValueIndexCondition EMPTY = create(null, null, null);

    public ValueIndexCondition(List<String> fieldNames, String indexName, String indexColumnNames) {
        if (fieldNames != null) {
            this.fieldNames = new ArrayList<>(fieldNames);
        }
        this.indexName = indexName;
        this.indexColumnNames = indexColumnNames;
    }


    public static ValueIndexCondition create(List<String> fieldNames, String indexName, String indexColumnNames) {
        return new ValueIndexCondition(fieldNames, indexName, indexColumnNames);
    }

    @Override
    public int compareTo(@NotNull ValueIndexCondition o) {
        return this.queryType.compareTo(o.queryType);
    }

    public boolean canPushDown() {
        return queryType != null;
    }

    public String getName() {
        return indexName;
    }

    public ValueIndexCondition withQueryType(QueryType queryType) {
        this.queryType = queryType;
        return this;
    }

    public ValueIndexCondition withRangeQueryLowerOp(ComparisonOperator rangeQueryLowerOp) {
        this.rangeQueryLowerOp = rangeQueryLowerOp;
        return this;
    }

    public ValueIndexCondition withRangeQueryLowerKey(List<Object> rangeQueryLowerKey) {
        this.rangeQueryLowerKey = normalize(rangeQueryLowerKey);
        return this;
    }

    public ValueIndexCondition withRangeQueryUpperOp(ComparisonOperator op) {
        this.rangeQueryUpperOp = op;
        return this;
    }

    public ValueIndexCondition withRangeQueryUpperKey(List<Object> rangeQueryUpperKey) {
        this.rangeQueryUpperKey = normalize(rangeQueryUpperKey);
        return this;
    }

    public ValueIndexCondition withPointQueryKey(List<Object> pointQueryKey) {
        this.pointQueryKey = normalize(pointQueryKey);
        return this;
    }

    @NotNull
    private static List<Object> normalize(List<Object> pointQueryKey) {
       return pointQueryKey;
    }

    public QueryType getQueryType() {
        return queryType == null ? QueryType.PK_FULL_SCAN : queryType;
    }

    @NotNull
    public static List<Partition> getObject(CustomRuleFunction customRuleFunction, ValueIndexCondition condition, List<Object> params) {
        if (condition == null) {
            return customRuleFunction.calculate(Collections.emptyMap());
        }
        Objects.requireNonNull(condition);
        List<Object> pointQueryKey = resolveParam(params, condition.getPointQueryKey());
        ComparisonOperator rangeQueryLowerOp = condition.getRangeQueryLowerOp();
        List<Object> rangeQueryLowerKey = resolveParam(params, condition.getRangeQueryLowerKey());
        ComparisonOperator rangeQueryUpperOp = condition.getRangeQueryUpperOp();
        List<Object> rangeQueryUpperKey = resolveParam(params, condition.getRangeQueryUpperKey());

        Map<String, Collection<RangeVariable>> map = new HashMap<>();
        Object o;
        switch (condition.getQueryType()) {
            case PK_POINT_QUERY:
                //queryByPrimaryKey
                for (Object o1 : pointQueryKey) {
                    RangeVariable rangeVariable = new RangeVariable(condition.getIndexColumnNames(), RangeVariableType.EQUAL, o1);
                    Collection<RangeVariable> rangeVariables = map.computeIfAbsent(condition.getIndexColumnNames(), (k) -> new ArrayList<>());
                    rangeVariables.add(rangeVariable);
                }

                return customRuleFunction.calculate(map);
            case PK_RANGE_QUERY:
                if (rangeQueryUpperOp == ComparisonOperator.LT) {
                    rangeQueryUpperOp = ComparisonOperator.LTE;
                }
                if (rangeQueryLowerOp == ComparisonOperator.GT) {
                    rangeQueryLowerOp = ComparisonOperator.GTE;
                }
                if (rangeQueryUpperOp == ComparisonOperator.LTE && rangeQueryLowerOp == ComparisonOperator.GTE) {
                    ArrayList<Object> leftValues = new ArrayList<>();
                    for (Object o1 : rangeQueryLowerKey) {
                        if (o1 instanceof RexNode) {
                            o1 = MycatRexUtil.resolveParam((RexNode) o1, params);
                        }
                        leftValues.add(o1);
                    }
                    ArrayList<Object> rightValues = new ArrayList<>();
                    for (Object o1 : rangeQueryUpperKey) {
                        if (o1 instanceof RexNode) {
                            o1 = MycatRexUtil.resolveParam((RexNode) o1, params);
                        }
                        rightValues.add(o1);
                    }
                    Collections.sort((List) rangeQueryLowerKey);
                    Collections.sort((List) rangeQueryUpperKey);

                    Object smallOne = rangeQueryLowerKey.get(0);
                    Object bigOne = rangeQueryUpperKey.get(rangeQueryUpperKey.size()-1);

                    RangeVariable rangeVariable = new RangeVariable(condition.getIndexColumnNames(), RangeVariableType.RANGE, smallOne, bigOne);
                    Collection<RangeVariable> rangeVariables = map.computeIfAbsent(condition.getIndexColumnNames(), (k) -> new ArrayList<>());
                    rangeVariables.add(rangeVariable);
                    return customRuleFunction.calculate(map);
                }

            case PK_FULL_SCAN:
            default:
                return customRuleFunction.calculate(Collections.emptyMap());
        }
    }


    public static List<Object> resolveParam(List<Object> params, List<Object> pointQueryKeyArg) {
        if (pointQueryKeyArg == null)return Collections.emptyList();
        ArrayList<Object> builder = new ArrayList<>();
        for (Object o : pointQueryKeyArg) {
            if (o instanceof RexDynamicParam){
                builder.add (params.get(((RexDynamicParam) o).getIndex()));
            }
            if (o instanceof RexCall&&((RexCall) o).getKind()== SqlKind.CAST){
                o = ((RexCall) o).getOperands().get(0);
            }
            if (o instanceof RexLiteral){
                RexLiteral rexLiteral = (RexLiteral) o;
                RelDataType type = rexLiteral.getType();
                switch (type.getSqlTypeName()) {
                    case BOOLEAN:
                        o = rexLiteral.getValueAs(Boolean.class);
                        break;
                    case INTEGER:
                    case SMALLINT:
                    case TINYINT:
                    case BIGINT:
                        o = rexLiteral.getValueAs(Long.class);
                        break;
                    case DECIMAL:
                        o = rexLiteral.getValueAs(BigDecimal.class);
                        break;
                    case FLOAT:
                    case REAL:
                    case DOUBLE:
                        o = rexLiteral.getValueAs(Double.class);
                        break;
                    case DATE:
                        o = rexLiteral.getValueAs(LocalDate.class);
                        break;
                    case TIMESTAMP:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        o = rexLiteral.getValueAs(LocalDateTime.class);
                        break;
                    case TIME:
                    case TIME_WITH_LOCAL_TIME_ZONE:
                    case CHAR:
                    case VARCHAR:
                    case BINARY:
                    case VARBINARY:
                    case NULL:
                    case ANY: ;
                    case SYMBOL:
                    case MULTISET:
                    case ARRAY:
                    case MAP:
                    case DISTINCT:
                    case STRUCTURED:
                    case ROW:
                    case OTHER:
                    case CURSOR:
                    case COLUMN_LIST:
                    case DYNAMIC_STAR:
                    case GEOMETRY:
                    case SARG:
                        o = rexLiteral.getValueAs(String.class);
                        break;
                }
                builder.add (o);
            }else {
                builder.add (o);
            }
        }

        return builder;
    }


}

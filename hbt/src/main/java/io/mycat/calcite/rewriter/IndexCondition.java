package io.mycat.calcite.rewriter;

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
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
@ToString
public class IndexCondition implements Comparable<IndexCondition>, Serializable {
    List<String> fieldNames;
    String indexName;
    String indexColumnNames;

    QueryType queryType;
    ComparisonOperator rangeQueryLowerOp;
    List<MycatDynamicParam> rangeQueryLowerKey;
    ComparisonOperator rangeQueryUpperOp;
    List<MycatDynamicParam> rangeQueryUpperKey;
    List<MycatDynamicParam> pointQueryKey;

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

    public static IndexCondition EMPTY = create(null, null, null);

    public IndexCondition(List<String> fieldNames, String indexName, String indexColumnNames) {
        if (fieldNames != null) {
            this.fieldNames = new ArrayList<>(fieldNames);
        }
        this.indexName = indexName;
        this.indexColumnNames = indexColumnNames;
    }


    public static IndexCondition create(List<String> fieldNames, String indexName, String indexColumnNames) {
        return new IndexCondition(fieldNames, indexName, indexColumnNames);
    }

    @Override
    public int compareTo(@NotNull IndexCondition o) {
        return this.queryType.compareTo(o.queryType);
    }

    public boolean canPushDown() {
        return queryType != null;
    }

    public String getName() {
        return indexName;
    }

    public IndexCondition withQueryType(QueryType queryType) {
        this.queryType = queryType;
        return this;
    }

    public IndexCondition withRangeQueryLowerOp(ComparisonOperator rangeQueryLowerOp) {
        this.rangeQueryLowerOp = rangeQueryLowerOp;
        return this;
    }

    public IndexCondition withRangeQueryLowerKey(List<Object> rangeQueryLowerKey) {
        this.rangeQueryLowerKey = normalize(rangeQueryLowerKey);
        return this;
    }

    public IndexCondition withRangeQueryUpperOp(ComparisonOperator op) {
        this.rangeQueryUpperOp = op;
        return this;
    }

    public IndexCondition withRangeQueryUpperKey(List<Object> rangeQueryUpperKey) {
        this.rangeQueryUpperKey = normalize(rangeQueryUpperKey);
        return this;
    }

    public IndexCondition withPointQueryKey(List<Object> pointQueryKey) {
        this.pointQueryKey = normalize(pointQueryKey);
        return this;
    }

    @NotNull
    private static List<MycatDynamicParam> normalize(List<Object> pointQueryKey) {
        ArrayList<MycatDynamicParam> res = new ArrayList<>();
        for (Object o : pointQueryKey) {
            if (o != null) {
                if (o instanceof RexDynamicParam) {
                    int index = ((RexDynamicParam) o).getIndex();
                    res.add(new MycatDynamicParam(index));
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        }
        return res;
    }

    public QueryType getQueryType() {
        return queryType == null ? QueryType.PK_FULL_SCAN : queryType;
    }

    @NotNull
    public static List<Partition> getObject(CustomRuleFunction customRuleFunction, IndexCondition condition, List<Object> params) {
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
                            o1 = io.mycat.calcite.rewriter.MycatRexUtil.resolveParam((RexNode) o1, params);
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

    @NotNull
    public static List<Object> resolveParam(List<Object> params, List<MycatDynamicParam> pointQueryKeyArg) {
        return Optional.ofNullable(pointQueryKeyArg)
                .orElse(Collections.emptyList()).stream().map(mycatDynamicParam -> params.get(mycatDynamicParam.index)).collect(Collectors.toList());
    }


}

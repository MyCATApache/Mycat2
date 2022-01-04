package io.mycat.calcite.rewriter;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.querycondition.ComparisonOperator;
import io.mycat.querycondition.KeyMeta;
import io.mycat.querycondition.QueryType;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class ValuePredicateAnalyzer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ValuePredicateAnalyzer.class);
    public static final RexBuilder REX_BUILDER = MycatCalciteSupport.RexBuilder;
    final String indexName;
    final List<KeyMeta> keyMetas;
    final List<String> fieldNames;

    public ValuePredicateAnalyzer(ShardingTable table, RelNode relNode) {
        this(null, table.keyMetas(), relNode.getRowType().getFieldNames());
    }

    public ValuePredicateAnalyzer(List<KeyMeta> keyMetas, List<String> fieldNames) {
        this(null, keyMetas, fieldNames);
    }

    public ValuePredicateAnalyzer(String indexName, List<KeyMeta> keyMetas, List<String> fieldNames) {
        this.indexName = indexName;
        this.keyMetas = keyMetas;
        this.fieldNames = fieldNames;
    }

    public Map<QueryType, List<ValueIndexCondition>> translateMatch(RexNode condition) {
        Map<QueryType, List<ValueIndexCondition>> tryAndInRes = tryAndIn(condition);
        if (tryAndInRes != null && !tryAndInRes.isEmpty()) return tryAndInRes;
        // does not support disjunctions
        List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
        if (disjunctions.size() == 1) {
            return translateAnd(disjunctions.get(0));
        } else if (disjunctions.isEmpty()) {
            return Collections.emptyMap();
        }
        RexNode listToInList = refactorOrListToInList(disjunctions);
        disjunctions = RelOptUtil.disjunctions(listToInList);
        if (disjunctions.size() == 1) {
            return translateAnd(disjunctions.get(0));
        }
        return Collections.emptyMap();
    }

    @Nullable
    private Map<QueryType, List<ValueIndexCondition>> tryAndIn(RexNode condition) {
        try {
            if (condition.isA(SqlKind.AND)) {
                RexCall rexCall = (RexCall) condition;
                List<RexNode> operands = rexCall.getOperands();
                RexNode primaryHead = operands.get(0);
                if (operands.subList(1, operands.size()).stream().anyMatch(i -> RexUtil.findOperatorCall(SqlStdOperatorTable.OR, i) != null)) {
                    return Collections.emptyMap();
                }
                boolean allMatch = true;
                RexNode primaryInputRef = null;
                List<RexNode> valueList = new ArrayList<>();
                if (primaryHead.isA(SqlKind.OR)) {
                    RexCall primaryNode = (RexCall) primaryHead;
                    for (RexNode maybeEquals : primaryNode.getOperands()) {
                        if (maybeEquals.isA(SqlKind.EQUALS)) {
                            RexCall equals = (RexCall) maybeEquals;
                            RexNode maybeId = equals.getOperands().get(0);
                            valueList.add(RexUtil.removeCast(equals.getOperands().get(1)));
                            if (maybeId.isA(SqlKind.INPUT_REF)) {
                                if (primaryInputRef == null) {
                                    primaryInputRef = maybeId;
                                    continue;
                                } else if (primaryInputRef.equals(maybeId)) {
                                    continue;
                                }
                            }
                        }
                        allMatch = false;
                        break;
                    }
                    if (allMatch&&!valueList.isEmpty()) {
                        Map<QueryType, List<ValueIndexCondition>> indexConditions = new HashMap<>();
                        ValueIndexCondition pushDownCondition = null;
                        for (KeyMeta skMeta : keyMetas) {
                            for (RexNode rexNode : valueList) {
                                if (pushDownCondition == null){
                                    pushDownCondition = findPushDownCondition(
                                            ImmutableList.of(MycatCalciteSupport.RexBuilder.makeCall(SqlStdOperatorTable.EQUALS, primaryInputRef, rexNode)), skMeta);
                                }else {
                                    pushDownCondition.pointQueryKey.add(rexNode);
                                }
                            }
                        }
                        if (pushDownCondition != null){
                            indexConditions.put(QueryType.PK_POINT_QUERY,Collections.singletonList(pushDownCondition));
                        }
                        return indexConditions;
                    }
                }
            }
            return Collections.emptyMap();
        } catch (Throwable throwable) {
            LOGGER.warn("", throwable);
        }
        return Collections.emptyMap();
    }

    private static RexNode refactorOrListToInList(List<RexNode> disjunctions) {
        LinkedList<RexNode> firstList = new LinkedList<>();
        LinkedList<RexNode> restList = new LinkedList<>();
        Collection<RexNode> lastList;
        Map<Boolean, List<RexNode>> map = disjunctions.stream().collect(Collectors.partitioningBy(k -> k.getKind() == SqlKind.EQUALS));
        List<RexNode> rexNodes = map.get(Boolean.TRUE);
        lastList = map.get(Boolean.FALSE);
        Map<RexInputRef, LinkedList<RexLiteral>> inMap = new HashMap<>();

        for (RexNode rexNode : rexNodes) {
            RexCall rexCall = (RexCall) rexNode;
            RexNode left = rexCall.getOperands().get(0);
            RexNode right = rexCall.getOperands().get(1);
            left = RexUtil.removeCast(left);
            right = RexUtil.removeCast(right);

            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                List<RexLiteral> inList = inMap.computeIfAbsent((RexInputRef) left, integer -> new LinkedList<>());
                inList.add((RexLiteral) right);
                continue;
            } else {
                restList.add(rexNode);
            }
        }
        for (Map.Entry<RexInputRef, LinkedList<RexLiteral>> e : inMap.entrySet()) {
            LinkedList<RexNode> value = (LinkedList) e.getValue();
            RexNode rexNode = MycatCalciteSupport.RexBuilder.makeIn(e.getKey(), value);
            firstList.add(rexNode);
        }

        List build = ImmutableList.builder().addAll(firstList).addAll(restList).addAll(lastList).build();
        if (build.size() == 1) {
            return (RexNode) build.get(0);
        }
        return MycatCalciteSupport.RexBuilder.makeCall(SqlStdOperatorTable.OR, build);
    }

    private Map<QueryType, List<ValueIndexCondition>> translateAnd(RexNode condition) {
        List<RexNode> rexNodeList = RelOptUtil.conjunctions(condition);


        List<ValueIndexCondition> indexConditions = new ArrayList<>();
        // try to push down filter by secondary keys
        for (KeyMeta skMeta : keyMetas) {
            indexConditions.add(findPushDownCondition(rexNodeList, skMeta));
        }

        return indexConditions.stream()
                .filter(i -> i != null)
                .filter(ValueIndexCondition::canPushDown)
                .filter(indexCondition -> nonForceIndexOrMatchForceIndexName(indexCondition.getName()))
                .sorted(Comparator.comparing(x -> x.getQueryType().priority()))
                .collect(Collectors.groupingBy(k -> k.getQueryType(),
                        Collectors.collectingAndThen(Collectors.toList(), indexConditions1 -> {
                            HashMap<String, ValueIndexCondition> conditionMap = new HashMap<>();
                            for (ValueIndexCondition newOne : indexConditions1) {
                                List<String> fieldNames = newOne.getIndexColumnNames();
                                for (String fieldName : fieldNames) {
                                    ValueIndexCondition oldOne = conditionMap.getOrDefault(fieldName, null);
                                    if (oldOne == null) {
                                        conditionMap.put(fieldName, newOne);
                                        continue;
                                    } else {
                                        if (newOne.getQueryType().compareTo(oldOne.getQueryType()) < 0) {
                                            conditionMap.put(fieldName, newOne);
                                        }
                                    }
                                }
                            }
                            return new ArrayList<>(conditionMap.values());
                        })));
    }

    private ValueIndexCondition findPushDownCondition(List<RexNode> rexNodeList, KeyMeta keyMeta) {
        // find field expressions matching index columns and specific operators
        List<InternalRexNode> matchedRexNodeList = analyzePrefixMatches(rexNodeList, keyMeta);

        // none of the conditions can be pushed down
        if (matchedRexNodeList.isEmpty()) {
            return ValueIndexCondition.EMPTY;
        }

        // a collection that maps ordinal in index column list
        // to multiple field expressions
        Multimap<Integer, InternalRexNode> keyOrdToNodesMap = HashMultimap.create();
        for (InternalRexNode node : matchedRexNodeList) {
            keyOrdToNodesMap.put(node.ordinalInKey, node);
        }

        // left-prefix index rule not match
        Collection<InternalRexNode> leftMostKeyNodes = keyOrdToNodesMap.values();
        if (leftMostKeyNodes.isEmpty()) {
            return ValueIndexCondition.EMPTY;
        }

        // create result which might have conditions to push down
        List<String> indexColumnNames = keyMeta.getColumnNames();
        List<RexNode> pushDownRexNodeList = new ArrayList<>();
        List<RexNode> remainderRexNodeList = new ArrayList<>(rexNodeList);
        ValueIndexCondition condition =
                ValueIndexCondition.create(fieldNames, keyMeta.getIndexName(), indexColumnNames);

        // handle point query if possible
        condition = handlePointQuery(condition, leftMostKeyNodes,
                keyOrdToNodesMap, pushDownRexNodeList, remainderRexNodeList);
        if (condition.canPushDown()) {
            return condition;
        }

        // handle range query
        condition = handleRangeQuery(condition, leftMostKeyNodes,
                pushDownRexNodeList, remainderRexNodeList, ">=", ">");
        condition = handleRangeQuery(condition, leftMostKeyNodes,
                pushDownRexNodeList, remainderRexNodeList, "<=", "<");

        return condition;
    }

    private static ValueIndexCondition handleRangeQuery(ValueIndexCondition condition,
                                                        Collection<InternalRexNode> leftMostKeyNodes,
                                                        List<RexNode> pushDownRexNodeList,
                                                        List<RexNode> remainderRexNodeList,
                                                        String... opList) {
        Optional<InternalRexNode> node = findFirstOp(leftMostKeyNodes, opList);
        if (node.isPresent()) {
            pushDownRexNodeList.add(node.get().node);
            remainderRexNodeList.remove(node.get().node);
            List<Object> key = createKey(Lists.newArrayList(node.get()));
            ComparisonOperator op = ComparisonOperator.parse(node.get().op);
            if (ComparisonOperator.isLowerBoundOp(opList)) {
                return condition
                        .withQueryType(QueryType.PK_RANGE_QUERY)
                        .withRangeQueryLowerOp(op)
                        .withRangeQueryLowerKey(key);
            } else if (ComparisonOperator.isUpperBoundOp(opList)) {
                return condition
                        .withQueryType(QueryType.PK_RANGE_QUERY)
                        .withRangeQueryUpperOp(op)
                        .withRangeQueryUpperKey(key);
            } else {
                throw new AssertionError("comparison operation is invalid " + op);
            }
        }
        return condition;
    }

    private static List<Object> createKey(List<InternalRexNode> nodes) {
        return nodes.stream().map(n -> n.right).collect(Collectors.toList());
    }

    private ValueIndexCondition handlePointQuery(ValueIndexCondition condition,
                                                 Collection<InternalRexNode> leftMostKeyNodes,
                                                 Multimap<Integer, InternalRexNode> keyOrdToNodesMap,
                                                 List<RexNode> pushDownRexNodeList,
                                                 List<RexNode> remainderRexNodeList) {
        Optional<InternalRexNode> leftMostEqOpNode = findFirstOp(leftMostKeyNodes, "=");
        if (leftMostEqOpNode.isPresent()) {
            InternalRexNode node = leftMostEqOpNode.get();

            List<InternalRexNode> matchNodes = Lists.newArrayList(node);
            findSubsequentMatches(matchNodes, 1, keyOrdToNodesMap, "=");
            List<Object> key = createKey(matchNodes);
            pushDownRexNodeList.add(node.node);
            remainderRexNodeList.remove(node.node);

            if (matchNodes.size() != 1) {
                // "=" operation does not apply on all index columns
                return condition
                        .withQueryType(QueryType.PK_POINT_QUERY)
                        .withPointQueryKey(key);
            } else {
                for (InternalRexNode n : matchNodes) {
                    pushDownRexNodeList.add(n.node);
                    remainderRexNodeList.remove(n.node);
                }
                return condition
                        .withQueryType(QueryType.PK_POINT_QUERY)
                        .withPointQueryKey(key);
            }
        }
        Optional<InternalRexNode> leftMostSargOpNode = findFirstOp(leftMostKeyNodes, "sarg");
        if (leftMostSargOpNode.isPresent()) {
            InternalRexNode node = leftMostSargOpNode.get();
            RexCall rexCall = (RexCall) node.node;
            Sarg sarg = (Sarg) ((RexLiteral) rexCall.getOperands().get(1)).getValue();
            if (sarg.isPoints()) {
                RexCall rexNode = (RexCall) RexUtil.expandSearch(REX_BUILDER, null, rexCall);

                List<Object> key = new ArrayList<>();
                rexNode.accept(new RexShuttle() {
                    @Override
                    public RexNode visitLiteral(RexLiteral literal) {
                        key.add(literal.getValue());
                        return super.visitLiteral(literal);
                    }
                });
                pushDownRexNodeList.add(node.node);
                remainderRexNodeList.remove(node.node);

                return condition
                        .withQueryType(QueryType.PK_POINT_QUERY)
                        .withPointQueryKey(key);
            } else {
                return condition.withQueryType(QueryType.PK_FULL_SCAN);
            }

        }
        return condition;
    }

    private static void findSubsequentMatches(List<InternalRexNode> nodes, int numOfKeyColumns,
                                              Multimap<Integer, InternalRexNode> keyOrdToNodesMap, String op) {
        for (int i = nodes.size(); i < numOfKeyColumns; i++) {
            Optional<InternalRexNode> eqOpNode = findFirstOp(keyOrdToNodesMap.get(i), op);
            if (eqOpNode.isPresent()) {
                nodes.add(eqOpNode.get());
            } else {
                break;
            }
        }
    }

    private static Optional<InternalRexNode> findFirstOp(Collection<InternalRexNode> nodes,
                                                         String... opList) {
        if (nodes.isEmpty()) {
            return Optional.empty();
        }
        for (InternalRexNode node : nodes) {
            for (String op : opList) {
                if (op.equals(node.op)) {
                    return Optional.of(node);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Analyzes from the first to the subsequent field expression following the
     * left-prefix rule, this will based on a specific index
     * (<code>KeyMeta</code>), check the column and its corresponding operation,
     * see if it can be translated into a push down condition.
     *
     * <p>The result is a collection of matched field expressions.
     *
     * @param rexNodeList Field expressions
     * @param keyMeta     Index metadata
     * @return a collection of matched field expressions
     */
    private List<InternalRexNode> analyzePrefixMatches(List<RexNode> rexNodeList, KeyMeta keyMeta) {
        return rexNodeList.stream()
                .map(rexNode -> translateMatch2(rexNode, keyMeta))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }


    /**
     * Internal representation of a row expression.
     */
    private static class InternalRexNode {
        /**
         * Relation expression node.
         */
        RexNode node;
        /**
         * Field ordinal in indexes.
         */
        int ordinalInKey;
        /**
         * Field name.
         */
        String fieldName;
        /**
         * Binary operation like =, >=, <=, > or <.
         */
        String op;
        /**
         * Binary operation right literal value.
         */
        Object right;
    }


    boolean nonForceIndexOrMatchForceIndexName(String name) {
        if (indexName == null) {
            return true;
        }
        return (indexName.equals(name));
    }

    /**
     * Translates a call to a binary operator, reversing arguments if
     * necessary.
     */
    private Optional<InternalRexNode> translateBinary(String op, String rop,
                                                      RexCall call, KeyMeta keyMeta) {
        final RexNode left = call.operands.get(0);
        final RexNode right = call.operands.get(1);
        Optional<InternalRexNode> expression =
                translateBinary2(op, left, right, call, keyMeta);
        if (expression.isPresent()) {
            return expression;
        }
        expression = translateBinary2(rop, right, left, call, keyMeta);
        return expression;
    }

    /**
     * Translates a call to a binary operator. Returns null on failure.
     */
    private Optional<InternalRexNode> translateBinary2(String op, RexNode left,
                                                       RexNode right, RexNode originNode, KeyMeta keyMeta) {
        RexNode rightLiteral;
        if (right.isA(SqlKind.LITERAL) || right.isA(SqlKind.DYNAMIC_PARAM)) {
            rightLiteral = right;
        } else {
            // because MySQL's TIMESTAMP is mapped to TIMESTAMP_WITH_TIME_ZONE sql type,
            // we should cast the value to literal.
            if (right.isA(SqlKind.CAST)) {
                rightLiteral = ((RexCall) right).operands.get(0);
            } else {
                return Optional.empty();
            }
        }
        switch (left.getKind()) {
            case INPUT_REF:
                final RexInputRef left1 = (RexInputRef) left;
                String name = fieldNames.get(left1.getIndex());
                // filter out field does not show in index column
                if (!keyMeta.findColumnName(name)) {
                    return Optional.empty();
                }
                return translateOp2(op, name, rightLiteral, originNode);
            case CAST:
                return translateBinary2(op, ((RexCall) left).operands.get(0), right,
                        originNode, keyMeta);
            default:
                return Optional.empty();
        }
    }

    private static boolean isSqlTypeMatch(RexCall rexCall, SqlTypeName sqlTypeName) {
        assert rexCall != null;
        return rexCall.type.getSqlTypeName() == sqlTypeName;
    }

    private Optional<InternalRexNode> translateMatch2(RexNode node, KeyMeta keyMeta) {
        switch (node.getKind()) {
            case EQUALS:
                return translateBinary("=", "=", (RexCall) node, keyMeta);
            case LESS_THAN:
                return translateBinary("<", ">", (RexCall) node, keyMeta);
            case LESS_THAN_OR_EQUAL:
                return translateBinary("<=", ">=", (RexCall) node, keyMeta);
            case GREATER_THAN:
                return translateBinary(">", "<", (RexCall) node, keyMeta);
            case GREATER_THAN_OR_EQUAL:
                return translateBinary(">=", "<=", (RexCall) node, keyMeta);
            case SEARCH: {
                return translateBinary("sarg", "sarg", (RexCall) node, keyMeta);
            }

            default:
                return Optional.empty();
        }
    }

    private Optional<InternalRexNode> translateOp2(String op, String name,
                                                   RexNode right, RexNode originNode) {

        InternalRexNode node = new InternalRexNode();
        node.node = originNode;
        node.ordinalInKey = fieldNames.indexOf(name);
        node.fieldName = name;
        node.op = op;
        node.right = right;
        return Optional.of(node);
    }


}

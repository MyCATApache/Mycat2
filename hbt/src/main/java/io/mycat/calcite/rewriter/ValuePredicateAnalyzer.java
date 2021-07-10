package io.mycat.calcite.rewriter;

import com.google.common.collect.HashMultimap;
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
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ValuePredicateAnalyzer {
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

    public ValueIndexCondition translateMatch(RexNode condition) {
        // does not support disjunctions
        List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
        if (disjunctions.size() == 1) {
            return translateAnd(disjunctions.get(0));
        } else {
            return ValueIndexCondition.EMPTY;
        }
    }

    private ValueIndexCondition translateAnd(RexNode condition) {
        // expand calls to SEARCH(..., Sarg()) to >, =, etc.
        final RexNode condition2 =
                RexUtil.expandSearch(REX_BUILDER, null, condition);
        // decompose condition by AND, flatten row expression
        List<RexNode> rexNodeList = RelOptUtil.conjunctions(condition2);

        List<ValueIndexCondition> indexConditions = new ArrayList<>();
        // try to push down filter by secondary keys
        for (KeyMeta skMeta : keyMetas) {
            indexConditions.add(findPushDownCondition(rexNodeList, skMeta));
        }
        // a collection of all possible push down conditions, see if it can
        // be pushed down, filter by forcing index name, then sort by comparator
        Stream<ValueIndexCondition> pushDownConditions = indexConditions.stream()
                .filter(i->i!=null)
                .filter(ValueIndexCondition::canPushDown)
                .filter(indexCondition -> nonForceIndexOrMatchForceIndexName(indexCondition.getName()))
                .sorted();

        return pushDownConditions.filter(i->i!=null).findFirst().orElse(ValueIndexCondition.EMPTY);
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
        if (right.isA(SqlKind.LITERAL)|| right.isA(SqlKind.DYNAMIC_PARAM))  {
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

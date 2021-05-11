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
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableMap;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.Distribution;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;


public class MycatHepJoinClustering
        extends RelRule<MycatHepJoinClustering.Config> implements TransformationRule{

    protected MycatHepJoinClustering(Config config) {
        super(config);
    }


    @Override
    public void onMatch(RelOptRuleCall call) {
        final MultiJoin multiJoinRel = call.rel(0);
        ArrayList<RelNode> sourceVertices = new ArrayList<>(multiJoinRel.getInputs());
        ArrayList<RelNode> targetVertices = new ArrayList<>(multiJoinRel.getInputs());
        targetVertices.sort(new ReorderComparator());
        if (targetVertices.equals(sourceVertices)) {
            return;
        }
        Map<Integer, Integer> sourcePosToTargetPos = getSourcePosToTargetPos(multiJoinRel, targetVertices);
        IdentityHashMap<RelNode, Integer> sourceFieldMap = getSourceOffsetMap(targetVertices);
        Mapping mapping = Mappings.bijection(getSourceFieldTargetField(multiJoinRel, sourceFieldMap));
        RexNode joinFilter = RexUtil.apply(mapping, multiJoinRel.getJoinFilter());
        joinFilter = reorder(joinFilter);
        RexNode postJoinFilter = null;
        if (multiJoinRel.getPostJoinFilter() != null) {
            postJoinFilter = multiJoinRel.getPostJoinFilter();
        }

        List<ImmutableBitSet> projFields = reorder(sourcePosToTargetPos, multiJoinRel.getProjFields());
        ImmutableMap<Integer, ImmutableIntList> newJoinFieldRefCountsMap = com.google.common.collect.ImmutableMap.copyOf(reorder(sourcePosToTargetPos, multiJoinRel.getJoinFieldRefCountsMap()));
        MultiJoin multiJoin = new MultiJoin(multiJoinRel.getCluster(), targetVertices, joinFilter, RelOptUtil.createProject(multiJoinRel, mapping).getRowType(), false,
                reorder(sourcePosToTargetPos, multiJoinRel.getOuterJoinConditions()), multiJoinRel.getJoinTypes(), projFields, newJoinFieldRefCountsMap, postJoinFilter);
//        RelBuilder builder = call.builder();
//        RelNode resRelNode = builder.push(RelOptUtil.createProject(multiJoin, mapping.inverse())).rename(multiJoinRel.getRowType().getFieldNames()).build();
        call.transformTo(RelOptUtil.createProject(multiJoin, mapping.inverse()));
    }

    private RexNode reorder(RexNode joinFilter) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(joinFilter);
        List<Set<Integer>> res = new ArrayList<>();
        Map<Integer, RexInputRef> ref = new HashMap<>();
        List<RexNode> postFilters = new ArrayList<>();
        for (RexNode conjunction : conjunctions) {
            if (conjunction.getKind() == SqlKind.EQUALS) {
                RexCall rexNode = (RexCall) conjunction;
                List<RexNode> operands = rexNode.getOperands();
                RexNode leftRexNode = operands.get(0);
                RexNode rightRexNode = operands.get(1);
                if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexInputRef) {
                    RexInputRef left = (RexInputRef) leftRexNode;
                    RexInputRef right = (RexInputRef) rightRexNode;
                    int leftIndex = left.getIndex();
                    int rightIndex = right.getIndex();

                    ref.put(leftIndex, left);
                    ref.put(rightIndex, right);

                    int key = Math.min(leftIndex, rightIndex);
                    int value = key == leftIndex ? rightIndex : leftIndex;

                    Optional<Set<Integer>> optional = res.stream().filter(c -> c.contains(key)).findFirst();
                    if (optional.isPresent()) {
                        optional.get().add(value);
                    } else {
                        optional = res.stream().filter(c -> c.contains(value)).findFirst();
                        if (optional.isPresent()) {
                            optional.get().add(key);
                        } else {
                            HashSet<Integer> set = new HashSet<>();
                            set.add(key);
                            set.add(value);
                            res.add(set);
                        }
                    }
                    continue;
                }
            }
            postFilters.add(conjunction);
        }
        RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;
        List<RexNode> ands = new ArrayList<>();
        for (Set<Integer> set : res) {
            List<Integer> integers = set.stream().sorted().collect(Collectors.toList());
            Integer target = integers.get(0);
            for (Integer other : integers.subList(1, integers.size())) {
                ands.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref.get(target), ref.get(other)));
            }
        }
        ands.addAll(postFilters);
        return RexUtil.composeConjunction(MycatCalciteSupport.RexBuilder, ands);
    }

    @NotNull
    private HashMap<Integer, Integer> getSourceFieldTargetField(MultiJoin multiJoinRel, IdentityHashMap<RelNode, Integer> sourceFieldMap) {
        HashMap<Integer, Integer> newFieldMap = new HashMap<>();
        int index = 0;
        for (int sourcePos = 0; sourcePos < multiJoinRel.getInputs().size(); sourcePos++) {
            RelNode relNode = multiJoinRel.getInputs().get(sourcePos);
            int fieldCount = relNode.getRowType().getFieldCount();
            for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                int i1 = sourceFieldMap.get(relNode);
                newFieldMap.put(index, fieldIndex + i1);
                index++;
            }
        }
        return newFieldMap;
    }

    @NotNull
    private IdentityHashMap<RelNode, Integer> getSourceOffsetMap(List<RelNode> newVertices) {
        IdentityHashMap<RelNode, Integer> sourceFieldMap = new IdentityHashMap<>();
        int count = 0;
        for (RelNode newVertex : newVertices) {
            sourceFieldMap.put(newVertex, count);
            count += newVertex.getRowType().getFieldCount();
        }
        return sourceFieldMap;
    }

    @NotNull
    private Map<Integer, Integer> getSourcePosToTargetPos(MultiJoin multiJoinRel, ArrayList<RelNode> newVertices) {
        Map<Integer, Integer> sourcePosToTargetPos = new HashMap<>();
        int index = 0;
        for (RelNode newVertex : newVertices) {
            sourcePosToTargetPos.put(multiJoinRel.getInputs().indexOf(newVertex), index);
            index++;
        }
        return sourcePosToTargetPos;
    }

    private <T> List<T> reorder(Map<Integer, Integer> sourceToNew, List<T> projFields) {
        Object[] objects = new Object[projFields.size()];
        Arrays.fill(objects, null);
        for (Map.Entry<Integer, Integer> integerIntegerEntry : sourceToNew.entrySet()) {
            objects[(integerIntegerEntry.getValue())] = projFields.get(integerIntegerEntry.getKey());
        }
        return (List) Arrays.asList(objects);
    }

    private <T> Map<Integer, T> reorder(Map<Integer, Integer> sourceToNew, Map<Integer, T> projFields) {
        Map<Integer, T> map = new HashMap<>();
        for (Map.Entry<Integer, Integer> integerIntegerEntry : sourceToNew.entrySet()) {
            map.put((integerIntegerEntry.getValue()), projFields.get(integerIntegerEntry.getKey()));
        }
        return map;
    }

    MycatView getView(RelNode relNode) {
        if (relNode instanceof HepRelVertex) {
            relNode = ((HepRelVertex) relNode).getCurrentRel();
        }
        if (relNode instanceof MycatView) {
            return (MycatView) relNode;
        } else {
            class Collector extends RelShuttleImpl {
                public MycatView view;

                public Collector() {

                }

                @Override
                public RelNode visit(RelNode other) {
                    if (other instanceof MycatView) {
                        MycatView next = (MycatView) other;
                        if (this.view != null) {
                            //todo
                        } else {
                            this.view = next;
                        }
                    }
                    return super.visit(other);
                }
            }

            Collector collector = new Collector();
            relNode.accept(collector);
            return collector.view;
        }
    }

    /**
     * Rule configuration.
     */
    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY
                .withOperandSupplier(b -> b.operand(MultiJoin.class).predicate(new Predicate<MultiJoin>() {
                    @Override
                    public boolean test(MultiJoin multiJoin) {
                        return multiJoin.getJoinTypes().stream().allMatch(t -> t == JoinRelType.INNER)
                                &&
                                multiJoin.getProjFields().stream().allMatch(p -> p == null);
                    }
                }).anyInputs())
                .as(Config.class);

        @Override
        default MycatHepJoinClustering toRule() {
            return new MycatHepJoinClustering(this);
        }
    }

    private class ReorderComparator implements Comparator<RelNode> {
        @Override
        public int compare(RelNode o1, RelNode o2) {
            MycatView left = MycatHepJoinClustering.this.getView(o1);
            MycatView right = MycatHepJoinClustering.this.getView(o2);
            if (left != null && right != null) {
                Distribution ldistribution = left.getDistribution();
                Distribution rdistribution = right.getDistribution();
                if (ldistribution.type() == rdistribution.type()) {
                    return 0;
                }
                if (ldistribution.type() == Distribution.Type.BroadCast) {
                    return 0;
                }
                if (rdistribution.type() == Distribution.Type.BroadCast) {
                    return 0;
                }
                if (ldistribution.type() == Distribution.Type.PHY) {
                    return 1;
                }
                if (rdistribution.type() == Distribution.Type.PHY) {
                    return -1;
                }
                return 0;
            }
            if (left != null) {
                return -1;
            }
            if (right != null) {
                return -1;
            }
            return 0;
        }
    }
}

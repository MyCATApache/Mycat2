/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Rules and relational operators for
 * {@link MycatConvention}
 * calling convention.
 * <p>
 * 1.注意点 转换时候注意目标的表达式是否能接受源表达式,比如有不支持的自定义函数,排序项,分组项
 */
public class MycatRules {
    final static BindableConvention convention = BindableConvention.INSTANCE;

    private MycatRules() {

    }

    protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

    static final RelFactories.ProjectFactory PROJECT_FACTORY =
            (input, hints, projects, fieldNames) -> {
                final RelOptCluster cluster = input.getCluster();
                final RelDataType rowType =
                        RexUtil.createStructType(cluster.getTypeFactory(), projects,
                                fieldNames, SqlValidatorUtil.F_SUGGESTER);
                return new MycatProject(cluster, input.getTraitSet(), input, projects,
                        rowType);
            };

    static final RelFactories.FilterFactory FILTER_FACTORY =
            (input, condition, variablesSet) -> {
                Preconditions.checkArgument(variablesSet.isEmpty(),
                        "MycatFilter does not allow variables");
                return new MycatFilter(input.getCluster(),
                        input.getTraitSet(), input, condition);
            };

    static final RelFactories.JoinFactory JOIN_FACTORY =
            (left, right, hints, condition, variablesSet, joinType, semiJoinDone) -> {
                final RelOptCluster cluster = left.getCluster();
                final RelTraitSet traitSet = cluster.traitSetOf(left.getConvention());
                try {
                    return new MycatJoin(cluster, traitSet, left, right, condition,
                            variablesSet, joinType);
                } catch (InvalidRelException e) {
                    throw new AssertionError(e);
                }
            };

    static final RelFactories.CorrelateFactory CORRELATE_FACTORY =
            (left, right, correlationId, requiredColumns, joinType) -> {
                throw new UnsupportedOperationException("MycatCorrelate");
            };

    public static final RelFactories.SortFactory SORT_FACTORY =
            (input, collation, offset, fetch) -> {
                throw new UnsupportedOperationException("MycatSort");
            };

    public static final RelFactories.ExchangeFactory EXCHANGE_FACTORY =
            (input, distribution) -> {
                throw new UnsupportedOperationException("MycatExchange");
            };

    public static final RelFactories.SortExchangeFactory SORT_EXCHANGE_FACTORY =
            (input, distribution, collation) -> {
                throw new UnsupportedOperationException("MycatSortExchange");
            };

    public static final RelFactories.AggregateFactory AGGREGATE_FACTORY =
            (input, hints, groupSet, groupSets, aggCalls) -> {
                final RelOptCluster cluster = input.getCluster();
                final RelTraitSet traitSet = cluster.traitSetOf(input.getConvention());
                return new MycatAggregate(cluster, traitSet, input, groupSet,
                        groupSets, aggCalls);
            };

    public static final RelFactories.MatchFactory MATCH_FACTORY =
            (input, pattern, rowType, strictStart, strictEnd, patternDefinitions,
             measures, after, subsets, allRows, partitionKeys, orderKeys,
             interval) -> {
                throw new UnsupportedOperationException("MycatMatch");
            };

    public static final RelFactories.SetOpFactory SET_OP_FACTORY =
            (kind, inputs, all) -> {
                RelNode input = inputs.get(0);
                RelOptCluster cluster = input.getCluster();
                final RelTraitSet traitSet = cluster.traitSetOf(input.getConvention());
                switch (kind) {
                    case UNION:
                        return new MycatUnion(cluster, traitSet, inputs, all);
                    case INTERSECT:
                        return new MycatIntersect(cluster, traitSet, inputs, all);
                    case EXCEPT:
                        return new MycatMinus(cluster, traitSet, inputs, all);
                    default:
                        throw new AssertionError("unknown: " + kind);
                }
            };

    public static final RelFactories.ValuesFactory VALUES_FACTORY =
            (cluster, rowType, tuples) -> {
                throw new UnsupportedOperationException();
            };

    public static final RelFactories.TableScanFactory TABLE_SCAN_FACTORY =
            (toRelContext, table) -> {
                throw new UnsupportedOperationException();
            };

    public static final RelFactories.SnapshotFactory SNAPSHOT_FACTORY =
            (input, period) -> {
                throw new UnsupportedOperationException();
            };

    /**
     * A {@link RelBuilderFactory} that creates a {@link RelBuilder} that will
     * create Mycat relational expressions for everything.
     */
    public static final RelBuilderFactory MYCAT_BUILDER =
            RelBuilder.proto(
                    Contexts.of(PROJECT_FACTORY,
                            FILTER_FACTORY,
                            JOIN_FACTORY,
                            SORT_FACTORY,
                            EXCHANGE_FACTORY,
                            SORT_EXCHANGE_FACTORY,
                            AGGREGATE_FACTORY,
                            MATCH_FACTORY,
                            SET_OP_FACTORY,
                            VALUES_FACTORY,
                            TABLE_SCAN_FACTORY,
                            SNAPSHOT_FACTORY));

    public static List<RelOptRule> rules() {
        return rules(MycatConvention.INSTANCE);
    }

    public static List<RelOptRule> rules(MycatConvention out) {
        return rules(out, RelFactories.LOGICAL_BUILDER);
    }

    public static List<RelOptRule> rules(MycatConvention out,
                                         RelBuilderFactory relBuilderFactory) {
        return ImmutableList.of(
                new MycatJoinRule(out, relBuilderFactory),
                new MycatCalcRule(out, relBuilderFactory),
                new MycatProjectRule(out, relBuilderFactory),
                new MycatFilterRule(out, relBuilderFactory),
                new MycatAggregateRule(out, relBuilderFactory),
                new MycatSortRule(out, relBuilderFactory),
                new MycatUnionRule(out, relBuilderFactory),
                new MycatIntersectRule(out, relBuilderFactory),
                new MycatMinusRule(out, relBuilderFactory),
                new MycatTableModificationRule(out, relBuilderFactory),
                new MycatValuesRule(out, relBuilderFactory));
    }

    /**
     * Abstract base class for rule that converts to Mycat.
     */
    abstract static class MycatConverterRule extends ConverterRule {
        protected final MycatConvention out;

        <R extends RelNode> MycatConverterRule(Class<R> clazz,
                                               Predicate<? super R> predicate, RelTrait in, MycatConvention out,
                                               RelBuilderFactory relBuilderFactory, String description) {
            super(clazz, predicate, in, out, relBuilderFactory, description);
            this.out = out;
        }
    }

    /**
     * Rule that converts a join to Mycat.
     */
    public static class MycatJoinRule extends MycatConverterRule {

        /**
         * Creates a MycatJoinRule.
         */
        public MycatJoinRule(MycatConvention out,
                             RelBuilderFactory relBuilderFactory) {
            super(Join.class, (Predicate<RelNode>) r -> true, convention,
                    out, relBuilderFactory, "MycatJoinRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            final Join join = (Join) rel;
            switch (join.getJoinType()) {
                case SEMI:
                case ANTI:
                    // It's not possible to convert semi-joins or anti-joins. They have fewer columns
                    // than regular joins.
                    return null;
                default:
                    return convert(join, true);
            }
        }

        /**
         * Converts a {@code Join} into a {@code MycatJoin}.
         *
         * @param join               Join operator to convert
         * @param convertInputTraits Whether to convert input to {@code join}'s
         *                           Mycat convention
         * @return A new MycatJoin
         */
        public RelNode convert(Join join, boolean convertInputTraits) {
            final List<RelNode> newInputs = new ArrayList<>();
            for (RelNode input : join.getInputs()) {
                if (convertInputTraits && input.getConvention() != getOutTrait()) {
                    input =
                            convert(input,
                                    input.getTraitSet().replace(out));
                }
                newInputs.add(input);
            }
            if (convertInputTraits && !canJoinOnCondition(join.getCondition())) {
                return null;
            }
            try {
                return new MycatJoin(
                        join.getCluster(),
                        join.getTraitSet().replace(out),
                        newInputs.get(0),
                        newInputs.get(1),
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType());
            } catch (InvalidRelException e) {

                return null;
            }
        }

        /**
         * Returns whether a condition is supported by {@link MycatJoin}.
         *
         * <p>Corresponds to the capabilities of
         * {@link SqlImplementor#convertConditionToSqlNode}.
         *
         * @param node Condition
         * @return Whether condition is supported
         */
        private boolean canJoinOnCondition(RexNode node) {
            final List<RexNode> operands;
            switch (node.getKind()) {
                case AND:
                case OR:
                    operands = ((RexCall) node).getOperands();
                    for (RexNode operand : operands) {
                        if (!canJoinOnCondition(operand)) {
                            return false;
                        }
                    }
                    return true;

                case EQUALS:
                case IS_NOT_DISTINCT_FROM:
                case NOT_EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    operands = ((RexCall) node).getOperands();
                    if ((operands.get(0) instanceof RexInputRef)
                            && (operands.get(1) instanceof RexInputRef)) {
                        return true;
                    }
                    // fall through

                default:
                    return false;
            }
        }
    }

    /**
     * Join operator implemented in Mycat convention.
     */
    public static class MycatJoin extends Join implements MycatRel {
        /**
         * Creates a MycatJoin.
         */
        public MycatJoin(RelOptCluster cluster, RelTraitSet traitSet,
                         RelNode left, RelNode right, RexNode condition,
                         Set<CorrelationId> variablesSet, JoinRelType joinType)
                throws InvalidRelException {
            super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
        }

        @Override
        public MycatJoin copy(RelTraitSet traitSet, RexNode condition,
                              RelNode left, RelNode right, JoinRelType joinType,
                              boolean semiJoinDone) {
            try {
                return new MycatJoin(getCluster(), traitSet, left, right,
                        condition, variablesSet, joinType);
            } catch (InvalidRelException e) {
                // Semantic error not possible. Must be a bug. Convert to
                // internal error.
                throw new AssertionError(e);
            }
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner,
                                          RelMetadataQuery mq) {
            // We always "build" the
            double rowCount = mq.getRowCount(this);

            return planner.getCostFactory().makeCost(rowCount, 0, 0);
        }

        @Override
        public double estimateRowCount(RelMetadataQuery mq) {
            final double leftRowCount = left.estimateRowCount(mq);
            final double rightRowCount = right.estimateRowCount(mq);
            return Math.max(leftRowCount, rightRowCount);
        }

      @Override
      public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatJoin").item("joinType",joinType).item("condition",condition).into();
        for (RelNode input : getInputs()) {
          MycatRel rel = (MycatRel)input;
          rel.explain(writer);
        }
        return writer.ret();
      }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }

    /**
     * Rule to convert a {@link Calc} to an
     * {@link MycatCalcRule}.
     */
    private static class MycatCalcRule extends MycatConverterRule {
        /**
         * Creates a MycatCalcRule.
         */
        private MycatCalcRule(MycatConvention out,
                              RelBuilderFactory relBuilderFactory) {
            super(Calc.class, (Predicate<RelNode>) r -> true, convention,
                    out, relBuilderFactory, "MycatCalcRule");
        }

        public RelNode convert(RelNode rel) {
            final Calc calc = (Calc) rel;

            // If there's a multiset, let FarragoMultisetSplitter work on it
            // first.
            if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
                return null;
            }

            return new MycatCalc(rel.getCluster(), rel.getTraitSet().replace(out),
                    convert(calc.getInput(), calc.getTraitSet().replace(out)),
                    calc.getProgram());
        }
    }

    /**
     * Calc operator implemented in Mycat convention.
     *
     * @see Calc
     */
    public static class MycatCalc extends SingleRel implements MycatRel {
        private final RexProgram program;

        public MycatCalc(RelOptCluster cluster,
                         RelTraitSet traitSet,
                         RelNode input,
                         RexProgram program) {
            super(cluster, traitSet, input);
            assert getConvention() instanceof MycatConvention;
            this.program = program;
            this.rowType = program.getOutputRowType();
        }

        public RelWriter explainTerms(RelWriter pw) {
            return program.explainCalc(super.explainTerms(pw));
        }

        @Override
        public double estimateRowCount(RelMetadataQuery mq) {
            return RelMdUtil.estimateFilteredRows(getInput(), program, mq);
        }

        public RelOptCost computeSelfCost(RelOptPlanner planner,
                                          RelMetadataQuery mq) {
            double dRows = mq.getRowCount(this);
            double dCpu = mq.getRowCount(getInput())
                    * program.getExprCount();
            double dIo = 0;
            return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new MycatCalc(getCluster(), traitSet, sole(inputs), program);
        }


      @Override
      public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatCalc").item("program", this.program).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
      }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }

    /**
     * Rule to convert a {@link Project} to
     * an {@link MycatProjectRule}.
     */
    public static class MycatProjectRule extends MycatConverterRule {

        /**
         * Creates a MycatProjectRule.
         */
        public MycatProjectRule(final MycatConvention out,
                                RelBuilderFactory relBuilderFactory) {
            super(Project.class, (Predicate<Project>) project ->
                            true,
                    convention, out, relBuilderFactory, "MycatProjectRule");
        }

        private static boolean userDefinedFunctionInProject(Project project) {
            CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
            for (RexNode node : project.getChildExps()) {
                node.accept(visitor);
                if (visitor.containsUserDefinedFunction()) {
                    return true;
                }
            }
            return false;
        }

        public RelNode convert(RelNode rel) {
            final Project project = (Project) rel;

            return new MycatProject(
                    rel.getCluster(),
                    rel.getTraitSet().replace(out),
                    convert(
                            project.getInput(),
                            project.getInput().getTraitSet().replace(out)),
                    project.getProjects(),
                    project.getRowType());
        }
    }

    /**
     * Implementation of {@link Project} in
     * {@link MycatConvention Mycat calling convention}.
     */
    public static class MycatProject
            extends Project
            implements MycatRel {
        public MycatProject(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                List<? extends RexNode> projects,
                RelDataType rowType) {
            super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
            assert getConvention() instanceof MycatConvention;
        }


        @Override
        public MycatProject copy(RelTraitSet traitSet, RelNode input,
                                 List<RexNode> projects, RelDataType rowType) {
            return new MycatProject(getCluster(), traitSet, input, projects, rowType);
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner,
                                          RelMetadataQuery mq) {
            return super.computeSelfCost(planner, mq)
                    .multiplyBy(MycatConvention.COST_MULTIPLIER);
        }



        @Override
        public ExplainWriter explain(ExplainWriter writer) {
          writer.name("MycatProject").item("projects", this.exps).into();
          ((MycatRel) getInput()).explain(writer);
          return writer.ret();
      }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }

    /**
     * Rule to convert a {@link Filter} to
     * an {@link MycatFilterRule}.
     */
    public static class MycatFilterRule extends MycatConverterRule {

        /**
         * Creates a MycatFilterRule.
         */
        public MycatFilterRule(MycatConvention out,
                               RelBuilderFactory relBuilderFactory) {
            super(Filter.class,
                    (Predicate<Filter>) r -> !userDefinedFunctionInFilter(r),
                    convention, out, relBuilderFactory, "MycatFilterRule");
        }

        private static boolean userDefinedFunctionInFilter(Filter filter) {
            CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
            filter.getCondition().accept(visitor);
            return visitor.containsUserDefinedFunction();
        }

        public RelNode convert(RelNode rel) {
            final Filter filter = (Filter) rel;

            return new MycatFilter(
                    rel.getCluster(),
                    rel.getTraitSet().replace(out),
                    convert(filter.getInput(),
                            filter.getInput().getTraitSet().replace(out)),
                    filter.getCondition());
        }
    }

    /**
     * Implementation of {@link Filter} in
     * {@link MycatConvention Mycat calling convention}.
     */
    public static class MycatFilter extends Filter implements MycatRel {
        public MycatFilter(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                RexNode condition) {
            super(cluster, traitSet, input, condition);
            assert getConvention() instanceof MycatConvention;
        }

        public MycatFilter copy(RelTraitSet traitSet, RelNode input,
                                RexNode condition) {
            return new MycatFilter(getCluster(), traitSet, input, condition);
        }

      @Override
      public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatFilter").item("condition", condition).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
      }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }

    /**
     * Rule to convert a {@link Aggregate}
     * to a {@link MycatAggregateRule}.
     */
    public static class MycatAggregateRule extends MycatConverterRule {

        /**
         * Creates a MycatAggregateRule.
         */
        public MycatAggregateRule(MycatConvention out,
                                  RelBuilderFactory relBuilderFactory) {
            super(Aggregate.class, (Predicate<RelNode>) r -> true, convention,
                    out, relBuilderFactory, "MycatAggregateRule");
        }

        public RelNode convert(RelNode rel) {
            final Aggregate agg = (Aggregate) rel;
            if (agg.getGroupSets().size() != 1) {
                // GROUPING SETS not supported; see
                // [CALCITE-734] Push GROUPING SETS to underlying SQL via Mycat adapter
                return null;
            }
            final RelTraitSet traitSet =
                    agg.getTraitSet().replace(out);
            return new MycatAggregate(rel.getCluster(), traitSet,
                    convert(agg.getInput(), out), agg.getGroupSet(),
                    agg.getGroupSets(), agg.getAggCallList());
        }
    }

    /**
     * Returns whether this Mycat data source can implement a given aggregate
     * function.
     */
    private static boolean canImplement(SqlAggFunction aggregation, SqlDialect sqlDialect) {
        return sqlDialect.supportsAggregateFunction(aggregation.getKind());
    }

    /**
     * Aggregate operator implemented in Mycat convention.
     */
    public static class MycatAggregate extends Aggregate implements MycatRel {
        public MycatAggregate(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                ImmutableBitSet groupSet,
                List<ImmutableBitSet> groupSets,
                List<AggregateCall> aggCalls) {
            super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
            assert getConvention() instanceof MycatConvention;
        }

        @Override
        public MycatAggregate copy(RelTraitSet traitSet, RelNode input,
                                   ImmutableBitSet groupSet,
                                   List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
            return new MycatAggregate(getCluster(), traitSet, input,
                    groupSet, groupSets, aggCalls);
        }


      @Override
      public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatAggregate").item("groupSets", groupSets).item("aggCalls", aggCalls).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
      }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }

    /**
     * Rule to convert a {@link Sort} to an
     * {@link MycatSortRule}.
     */
    public static class MycatSortRule extends MycatConverterRule {

        /**
         * Creates a MycatSortRule.
         */
        public MycatSortRule(MycatConvention out,
                             RelBuilderFactory relBuilderFactory) {
            super(Sort.class, (Predicate<RelNode>) r -> true, convention, out,
                    relBuilderFactory, "MycatSortRule");
        }

        public RelNode convert(RelNode rel) {
            return convert((Sort) rel, true);
        }

        /**
         * Converts a {@code Sort} into a {@code MycatSort}.
         *
         * @param sort               Sort operator to convert
         * @param convertInputTraits Whether to convert input to {@code sort}'s
         *                           Mycat convention
         * @return A new MycatSort
         */
        public RelNode convert(Sort sort, boolean convertInputTraits) {
            final RelTraitSet traitSet = sort.getTraitSet().replace(out);

            final RelNode input;
            if (convertInputTraits) {
                final RelTraitSet inputTraitSet = sort.getInput().getTraitSet().replace(out);
                input = convert(sort.getInput(), inputTraitSet);
            } else {
                input = sort.getInput();
            }

            return new MycatSort(sort.getCluster(), traitSet,
                    input, sort.getCollation(), sort.offset, sort.fetch);
        }
    }

    /**
     * Sort operator implemented in Mycat convention.
     */
    public static class MycatSort
            extends Sort
            implements MycatRel {
        public MycatSort(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                RelCollation collation,
                RexNode offset,
                RexNode fetch) {
            super(cluster, traitSet, input, collation, offset, fetch);
            assert getConvention() instanceof MycatConvention;
            assert getConvention() == input.getConvention();
        }

        @Override
        public MycatSort copy(RelTraitSet traitSet, RelNode newInput,
                              RelCollation newCollation, RexNode offset, RexNode fetch) {
            return new MycatSort(getCluster(), traitSet, newInput, newCollation,
                    offset, fetch);
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner,
                                          RelMetadataQuery mq) {
            return super.computeSelfCost(planner, mq).multiplyBy(0.9);
        }


        @Override
        public ExplainWriter explain(ExplainWriter writer) {
            writer.name("MycatSort").item("offset", offset).item("limit", fetch).into();
            ((MycatRel) getInput()).explain(writer);
            return writer.ret();
        }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }

    /**
     * Rule to convert an {@link Union} to a
     * {@link MycatUnionRule}.
     */
    public static class MycatUnionRule extends MycatConverterRule {

        /**
         * Creates a MycatUnionRule.
         */
        public MycatUnionRule(MycatConvention out,
                              RelBuilderFactory relBuilderFactory) {
            super(Union.class, (Predicate<RelNode>) r -> true, convention, out,
                    relBuilderFactory, "MycatUnionRule");
        }

        public RelNode convert(RelNode rel) {
            final Union union = (Union) rel;
            final RelTraitSet traitSet =
                    union.getTraitSet().replace(out);
            return new MycatUnion(rel.getCluster(), traitSet,
                    convertList(union.getInputs(), out), union.all);
        }
    }

    /**
     * Union operator implemented in Mycat convention.
     */
    public static class MycatUnion extends Union implements MycatRel {
        public MycatUnion(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                List<RelNode> inputs,
                boolean all) {
            super(cluster, traitSet, inputs, all);
        }

        public MycatUnion copy(
                RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
            return new MycatUnion(getCluster(), traitSet, inputs, all);
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner,
                                          RelMetadataQuery mq) {
            return super.computeSelfCost(planner, mq).multiplyBy(.1);
        }


      @Override
      public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatUnion").into();
        for (RelNode input : getInputs()) {
          MycatRel rel = (MycatRel)input;
          rel.explain(writer);
        }
        return writer.ret();
      }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }

    /**
     * Rule to convert a {@link Intersect}
     * to a {@link MycatIntersectRule}.
     */
    public static class MycatIntersectRule extends MycatConverterRule {
        /**
         * Creates a MycatIntersectRule.
         */
        private MycatIntersectRule(MycatConvention out,
                                   RelBuilderFactory relBuilderFactory) {
            super(Intersect.class, (Predicate<RelNode>) r -> true, convention,
                    out, relBuilderFactory, "MycatIntersectRule");
        }

        public RelNode convert(RelNode rel) {
            final Intersect intersect = (Intersect) rel;
            if (intersect.all) {
                return null; // INTERSECT ALL not implemented
            }
            final RelTraitSet traitSet =
                    intersect.getTraitSet().replace(out);
            return new MycatIntersect(rel.getCluster(), traitSet,
                    convertList(intersect.getInputs(), out), false);
        }
    }

    /**
     * Intersect operator implemented in Mycat convention.
     */
    public static class MycatIntersect
            extends Intersect
            implements MycatRel {
        public MycatIntersect(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                List<RelNode> inputs,
                boolean all) {
            super(cluster, traitSet, inputs, all);
            assert !all;
        }

        public MycatIntersect copy(
                RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
            return new MycatIntersect(getCluster(), traitSet, inputs, all);
        }


      @Override
      public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatIntersect").into();
        for (RelNode input : getInputs()) {
          MycatRel rel = (MycatRel)input;
          rel.explain(writer);
        }
        return writer.ret();
      }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }

    /**
     * Rule to convert a {@link Minus} to a
     * {@link MycatMinusRule}.
     */
    public static class MycatMinusRule extends MycatConverterRule {
        /**
         * Creates a MycatMinusRule.
         */
        private MycatMinusRule(MycatConvention out,
                               RelBuilderFactory relBuilderFactory) {
            super(Minus.class, (Predicate<RelNode>) r -> true, convention, out,
                    relBuilderFactory, "MycatMinusRule");
        }

        public RelNode convert(RelNode rel) {
            final Minus minus = (Minus) rel;
            if (minus.all) {
                return null; // EXCEPT ALL not implemented
            }
            final RelTraitSet traitSet =
                    rel.getTraitSet().replace(out);
            return new MycatMinus(rel.getCluster(), traitSet,
                    convertList(minus.getInputs(), out), false);
        }
    }

    /**
     * Minus operator implemented in Mycat convention.
     */
    public static class MycatMinus extends Minus implements MycatRel {
        public MycatMinus(RelOptCluster cluster, RelTraitSet traitSet,
                          List<RelNode> inputs, boolean all) {
            super(cluster, traitSet, inputs, all);
            assert !all;
        }

        public MycatMinus copy(RelTraitSet traitSet, List<RelNode> inputs,
                               boolean all) {
            return new MycatMinus(getCluster(), traitSet, inputs, all);
        }

      @Override
      public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatMinus").into();
        for (RelNode input : getInputs()) {
          MycatRel rel = (MycatRel)input;
          rel.explain(writer);
        }
        return writer.ret();
      }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }

    /**
     * Rule that converts a table-modification to Mycat.
     */
    public static class MycatTableModificationRule extends MycatConverterRule {
        /**
         * Creates a MycatTableModificationRule.
         */
        private MycatTableModificationRule(MycatConvention out,
                                           RelBuilderFactory relBuilderFactory) {
            super(TableModify.class, (Predicate<RelNode>) r -> true,
                    convention, out, relBuilderFactory, "MycatTableModificationRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            final TableModify modify =
                    (TableModify) rel;
            final ModifiableTable modifiableTable =
                    modify.getTable().unwrap(ModifiableTable.class);
            if (modifiableTable == null) {
                return null;
            }
            final RelTraitSet traitSet =
                    modify.getTraitSet().replace(out);
            return new MycatTableModify(
                    modify.getCluster(), traitSet,
                    modify.getTable(),
                    modify.getCatalogReader(),
                    convert(modify.getInput(), traitSet),
                    modify.getOperation(),
                    modify.getUpdateColumnList(),
                    modify.getSourceExpressionList(),
                    modify.isFlattened());
        }
    }

    /**
     * Table-modification operator implemented in Mycat convention.
     */
    public static class MycatTableModify extends TableModify implements MycatRel {
        private final Expression expression;

        public MycatTableModify(RelOptCluster cluster,
                                RelTraitSet traitSet,
                                RelOptTable table,
                                Prepare.CatalogReader catalogReader,
                                RelNode input,
                                Operation operation,
                                List<String> updateColumnList,
                                List<RexNode> sourceExpressionList,
                                boolean flattened) {
            super(cluster, traitSet, table, catalogReader, input, operation,
                    updateColumnList, sourceExpressionList, flattened);
            assert input.getConvention() instanceof MycatConvention;
            assert getConvention() instanceof MycatConvention;
            final ModifiableTable modifiableTable =
                    table.unwrap(ModifiableTable.class);
            if (modifiableTable == null) {
                throw new AssertionError(); // TODO: user error in validator
            }
            this.expression = table.getExpression(Queryable.class);
            if (expression == null) {
                throw new AssertionError(); // TODO: user error in validator
            }
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner,
                                          RelMetadataQuery mq) {
            return super.computeSelfCost(planner, mq).multiplyBy(.1);
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new MycatTableModify(
                    getCluster(), traitSet, getTable(), getCatalogReader(),
                    sole(inputs), getOperation(), getUpdateColumnList(),
                    getSourceExpressionList(), isFlattened());
        }


      @Override
      public ExplainWriter explain(ExplainWriter writer) {
     throw new UnsupportedOperationException();
      }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }

    /**
     * Rule that converts a values operator to Mycat.
     */
    public static class MycatValuesRule extends MycatConverterRule {
        /**
         * Creates a MycatValuesRule.
         */
        private MycatValuesRule(MycatConvention out,
                                RelBuilderFactory relBuilderFactory) {
            super(Values.class, (Predicate<RelNode>) r -> true, convention,
                    out, relBuilderFactory, "MycatValuesRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            Values values = (Values) rel;
            return new MycatValues(values.getCluster(), values.getRowType(),
                    values.getTuples(), values.getTraitSet().replace(out));
        }
    }

    /**
     * Values operator implemented in Mycat convention.
     */
    public static class MycatValues extends Values implements MycatRel {
        MycatValues(RelOptCluster cluster, RelDataType rowType,
                    ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
            super(cluster, rowType, tuples, traitSet);
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            assert inputs.isEmpty();
            return new MycatValues(getCluster(), rowType, tuples, traitSet);
        }


        @Override
        public ExplainWriter explain(ExplainWriter writer) {
        return   writer.name("MycatValues").item("values",tuples).ret();
        }

        @Override
        public MycatExecutor implement(MycatExecutorImplementor implementor) {
            return null;
        }
    }


    /**
     * Visitor for checking whether part of projection is a user defined function or not
     */
    private static class CheckingUserDefinedFunctionVisitor extends RexVisitorImpl<Void> {

        private boolean containsUsedDefinedFunction = false;

        CheckingUserDefinedFunctionVisitor() {
            super(true);
        }

        public boolean containsUserDefinedFunction() {
            return containsUsedDefinedFunction;
        }

        @Override
        public Void visitCall(RexCall call) {
            SqlOperator operator = call.getOperator();
            if (operator instanceof SqlFunction
                    && ((SqlFunction) operator).getFunctionType().isUserDefined()) {
                containsUsedDefinedFunction |= true;
            }
            return super.visitCall(call);
        }

    }
}

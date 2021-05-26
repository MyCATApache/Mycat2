package io.mycat.calcite.localrel;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.logical.LocalToMycatConverterRule;
import io.mycat.calcite.logical.LocalToMycatRelConverter;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.SQLRBORewriter;
import io.mycat.calcite.table.AbstractMycatTable;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Consumer;

public class LocalRules {
    public static final RelBuilderFactory LOCAL_BUILDER =
            RelBuilder.proto(
                    Contexts.of(LocalAggregate.AGGREGATE_FACTORY,
                            LocalJoin.JOIN_FACTORY,
                            LocalSort.SORT_FACTORY,
                            LocalAggregate.AGGREGATE_FACTORY,
                            LocalUnion.SET_OP_FACTORY,
                            LocalValues.VALUES_FACTORY,
                            LocalTableScan.TABLE_SCAN_FACTORY));

    public static final List<RelOptRule> RULES = ImmutableList.of(
            LocalRules.ToLocalTableScanRule.DEFAULT_CONFIG.toRule(),
            LocalRules.ToViewTableScanRule.DEFAULT_CONFIG.toRule(),
            LocalRules.FilterViewRule.DEFAULT_CONFIG.toRule(),
            LocalRules.ProjectViewRule.DEFAULT_CONFIG.toRule(),
            LocalRules.AggViewRule.DEFAULT_CONFIG.toRule(),
            LocalRules.SortViewRule.DEFAULT_CONFIG.toRule(),
            LocalRules.JoinViewRule.DEFAULT_CONFIG.toRule(),
            LocalRules.CalcViewRule.DEFAULT_CONFIG.toRule()
    );

    public static class ToLocalTableScanRule extends ConverterRule {


        protected ToLocalTableScanRule(ConverterRule.Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            return LocalTableScan.create((LogicalTableScan) rel);
        }

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalTableScan.class, Convention.NONE,
                        LocalConvention.INSTANCE, "ToLocalTableScanRule")
                .as(ToLocalTableScanRule.Config.class)
                .withRuleFactory(ToLocalTableScanRule::new);
    }

    public static class ToViewTableScanRule extends ConverterRule {


        protected ToViewTableScanRule(ConverterRule.Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            final LocalTableScan logicalTableScan = (LocalTableScan) rel;
            AbstractMycatTable abstractMycatTable = logicalTableScan.getTable().unwrap(AbstractMycatTable.class);
            return MycatView.ofBottom(logicalTableScan, abstractMycatTable.createDistribution());
        }

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LocalTableScan.class, LocalConvention.INSTANCE,
                        MycatConvention.INSTANCE, "ToViewTableScanRule")
                .as(ToViewTableScanRule.Config.class)
                .withRuleFactory(ToViewTableScanRule::new);
    }

    public static class FilterViewRule extends RelRule<FilterViewRule.Config> {

        public FilterViewRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Filter filter = call.rel(0);
            MycatView view = call.rel(1);
            SQLRBORewriter.view(view, LocalFilter.create(filter, view)).ifPresent(new Consumer<RelNode>() {
                @Override
                public void accept(RelNode res) {
                    ToLogicalConverter toLogicalConverter = getToLogicalConverter(res);
                    call.transformTo(res.accept(toLogicalConverter));
                }
            });
        }

        public static final Config DEFAULT_CONFIG = Config.EMPTY.as(FilterViewRule.Config.class).withOperandFor();

        public interface Config extends RelRule.Config {
            @Override
            default FilterViewRule toRule() {
                return new FilterViewRule(this);
            }


            default FilterViewRule.Config withOperandFor() {
                return withOperandSupplier(b0 ->
                        b0.operand(Filter.class).oneInput(b1 -> b1.operand(MycatView.class).noInputs()))
                        .withDescription("FilterViewRule")
                        .as(FilterViewRule.Config.class);
            }
        }
    }

    public static class ProjectViewRule extends RelRule<ProjectViewRule.Config> {

        public static final Config DEFAULT_CONFIG = Config.EMPTY.as(ProjectViewRule.Config.class).withOperandFor();

        public ProjectViewRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Project project = call.rel(0);
            MycatView view = call.rel(1);
            SQLRBORewriter.view(view, LocalProject.create(project, view)).ifPresent(res -> {
                ToLogicalConverter toLogicalConverter = getToLogicalConverter(res);
                call.transformTo(res.accept(toLogicalConverter));
            });
        }

        public interface Config extends RelRule.Config {
            @Override
            default ProjectViewRule toRule() {
                return new ProjectViewRule(this);
            }


            default ProjectViewRule.Config withOperandFor() {
                return withOperandSupplier(b0 ->
                        b0.operand(Project.class).oneInput(b1 -> b1.operand(MycatView.class).noInputs()))
                        .withDescription("ProjectViewRule")
                        .as(ProjectViewRule.Config.class);
            }
        }
    }

    public static class JoinViewRule extends RelRule<JoinViewRule.Config> {

        public static final Config DEFAULT_CONFIG = Config.EMPTY.as(JoinViewRule.Config.class).withOperandFor();

        public JoinViewRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Join join = call.rel(0);
            MycatView left = call.rel(1);
            MycatView right = call.rel(2);
            SQLRBORewriter.bottomJoin(left, right, LocalJoin.create(join, left, right)).ifPresent(new Consumer<RelNode>() {
                @Override
                public void accept(RelNode rel) {
                    ToLogicalConverter toLogicalConverter = getToLogicalConverter(rel);
                    call.transformTo(rel.accept(toLogicalConverter));
                }
            });
        }

        public interface Config extends RelRule.Config {
            @Override
            default JoinViewRule toRule() {
                return new JoinViewRule(this);
            }

            default JoinViewRule.Config withOperandFor() {
                return withOperandSupplier(b0 ->
                        b0.operand(Join.class).inputs(b1 -> b1.operand(MycatView.class).noInputs(), b2 -> b2.operand(MycatView.class).noInputs()))
                        .withDescription("JoinViewRule")
                        .as(JoinViewRule.Config.class);
            }
        }
    }

    public static class AggViewRule extends RelRule<AggViewRule.Config> {

        public static final Config DEFAULT_CONFIG = Config.EMPTY.as(AggViewRule.Config.class).withOperandFor();

        public AggViewRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Aggregate aggregate = call.rel(0);
            MycatView input = call.rel(1);
            SQLRBORewriter.aggregate(input, LocalAggregate.create(aggregate, input)).ifPresent(new Consumer<RelNode>() {
                @Override
                public void accept(RelNode res) {
                    ToLogicalConverter toLogicalConverter = getToLogicalConverter(res);
                    call.transformTo(res.accept(toLogicalConverter));
                }
            });
        }

        public interface Config extends RelRule.Config {
            @Override
            default AggViewRule toRule() {
                return new AggViewRule(this);
            }

            default AggViewRule.Config withOperandFor() {
                return withOperandSupplier(b0 ->
                        b0.operand(Aggregate.class).oneInput(b1 -> b1.operand(MycatView.class).noInputs()))
                        .withDescription("AggViewRule")
                        .as(AggViewRule.Config.class);
            }
        }
    }


    public static class SortViewRule extends RelRule<SortViewRule.Config> {

        public static final Config DEFAULT_CONFIG = Config.EMPTY.as(SortViewRule.Config.class).withOperandFor();

        public SortViewRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Sort sort = call.rel(0);
            MycatView input = call.rel(1);
            SQLRBORewriter.sort(input, LocalSort.create(sort, input)).ifPresent(new Consumer<RelNode>() {
                @Override
                public void accept(RelNode res) {
                    ToLogicalConverter toLogicalConverter = getToLogicalConverter(res);
                    call.transformTo(res.accept(toLogicalConverter));
                }
            });
        }

        public interface Config extends RelRule.Config {
            @Override
            default SortViewRule toRule() {
                return new SortViewRule(this);
            }

            default SortViewRule.Config withOperandFor() {
                return withOperandSupplier(b0 ->
                        b0.operand(Sort.class).oneInput(b1 -> b1.operand(MycatView.class).noInputs()))
                        .withDescription("SortViewRule")
                        .as(SortViewRule.Config.class);
            }
        }
    }

    public static class CalcViewRule extends RelRule<CalcViewRule.Config> {

        public static final Config DEFAULT_CONFIG = Config.EMPTY.as(CalcViewRule.Config.class).withOperandFor();

        public CalcViewRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Calc calc = call.rel(0);
            MycatView input = call.rel(1);
            SQLRBORewriter.view(input, LocalCalc.create(calc, input)).ifPresent(res -> {
                ToLogicalConverter toLogicalConverter = getToLogicalConverter(res);
                call.transformTo(res.accept(toLogicalConverter));
            });
        }

        public interface Config extends RelRule.Config {
            @Override
            default CalcViewRule toRule() {
                return new CalcViewRule(this);
            }

            default CalcViewRule.Config withOperandFor() {
                return withOperandSupplier(b0 ->
                        b0.operand(Calc.class).oneInput(b1 -> b1.operand(MycatView.class).noInputs()))
                        .withDescription("CalcViewRule")
                        .as(CalcViewRule.Config.class);
            }
        }
    }

    @NotNull
    private static ToLogicalConverter getToLogicalConverter(RelNode res) {
        ToLogicalConverter toLogicalConverter = new ToLogicalConverter(MycatCalciteSupport.relBuilderFactory.create(res.getCluster(), null)){
            @Override
            public RelNode visit(RelNode relNode) {
                if (relNode instanceof MycatView){
                    return relNode;
                }
                return super.visit(relNode);
            }
        };
        return toLogicalConverter;
    }
}

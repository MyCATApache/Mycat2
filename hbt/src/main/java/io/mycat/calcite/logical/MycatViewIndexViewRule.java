package io.mycat.calcite.logical;

import com.google.common.collect.ImmutableMap;
import io.mycat.calcite.localrel.LocalRules;
import io.mycat.calcite.table.ShardingTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class MycatViewIndexViewRule extends RelRule<MycatViewIndexViewRule.Config> {

    public static final MycatViewIndexViewRule.Config DEFAULT_CONFIG = LocalRules.CalcViewRule.Config.EMPTY.as(MycatViewIndexViewRule.Config.class).withOperandFor();

    public MycatViewIndexViewRule(MycatViewIndexViewRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MycatView mycatRel = call.rel(0);
        RelDataType rowType = mycatRel.getRowType();
        ImmutableMap.Builder<RelNode, RelNode> builder = ImmutableMap.builder();


        RelNode firstRelNode = mycatRel.getRelNode();
        RelOptCluster cluster = firstRelNode.getCluster();
        List<RelNode> relNodes = Collections.emptyList();
        if (firstRelNode instanceof TableScan) {

        } else if (firstRelNode instanceof Filter) {
            Filter filter = (Filter) firstRelNode;
            RelNode secondRelNode = filter.getInput();
            RexNode condition = filter.getCondition();
            if (secondRelNode instanceof TableScan) {
                TableScan tableScan = (TableScan) secondRelNode;
                relNodes = MycatView.produceIndexViews((TableScan) secondRelNode, condition, tableScan.identity(), rowType);
            } else if (secondRelNode instanceof Project) {
                Project project = (Project) secondRelNode;
                if (project.getInput() instanceof TableScan) {
                    TableScan tableScan = (TableScan) project.getInput();
                    Optional<List<Integer>> projectIntList = getProjectIntList(project);
                    if (projectIntList.isPresent()) {
                        relNodes = MycatView.produceIndexViews(tableScan, condition, projectIntList.get(), rowType);
                    }

                }
            }
        } else if (firstRelNode instanceof Project) {
            Project project = (Project) firstRelNode;
            RelNode secondRelNode = project.getInput();

            if (secondRelNode instanceof TableScan) {

            } else if (secondRelNode instanceof Filter) {
                Filter filter = (Filter) secondRelNode;
                RelNode input = filter.getInput();
                if (input instanceof TableScan) {
                    TableScan tableScan = (TableScan) input;
                    Optional<List<Integer>> projectIntList = getProjectIntList(project);
                    if (projectIntList.isPresent()) {
                        relNodes = MycatView.produceIndexViews(tableScan, filter.getCondition(), projectIntList.get(), rowType);
                    }
                }
            }
        }
        for (RelNode relNode : relNodes) {
            builder.put(relNode, mycatRel);
        }
        call.transformTo(mycatRel, builder.build());
    }

    public static Optional<List<Integer>> getProjectIntList(Project project) {
        if (project.isMapping()) {
            final List<Integer> selectedColumns = new ArrayList<>();
            final RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
                public Void visitInputRef(RexInputRef inputRef) {
                    if (!selectedColumns.contains(inputRef.getIndex())) {
                        selectedColumns.add(inputRef.getIndex());
                    }
                    return null;
                }
            };
            visitor.visitEach(project.getProjects());
            return Optional.ofNullable(selectedColumns);
        }
        return Optional.empty();


    }

    @Nullable
    private static ImmutableIntList getProjectIntList(Mappings.TargetMapping mapping) {
        ImmutableIntList intList;
        if (mapping != null) {
            ArrayList<Integer> intListBuilder = new ArrayList<>();
            for (IntPair intPair : mapping) {
                intListBuilder.add(intPair.source);
            }
            intList = ImmutableIntList.copyOf(intListBuilder);
        } else {
            intList = null;
        }
        return intList;
    }

    public interface Config extends RelRule.Config {
        @Override
        default MycatViewIndexViewRule toRule() {
            return new MycatViewIndexViewRule(this);
        }

        default MycatViewIndexViewRule.Config withOperandFor() {
            return withOperandSupplier(b0 ->
                    b0.operand(MycatView.class).predicate(mycatView -> {
                        List<ShardingTable> shardingTables = mycatView.getDistribution().getShardingTables();
                        if (!shardingTables.isEmpty()) {
                            if (!shardingTables.get(0).getIndexTables().isEmpty()) {
                                RelNode relNode = mycatView.getRelNode();
                                if (relNode instanceof Filter) {
                                    RelNode project = ((Filter) relNode).getInput();
                                    if (project instanceof Project) {
                                        return ((Project) project).getInput() instanceof TableScan;
                                    }
                                }
                                if (relNode instanceof Filter) {
                                    return ((Filter) relNode).getInput() instanceof TableScan;
                                }
                                if (relNode instanceof Project) {
                                    RelNode filter = ((Project) relNode).getInput();
                                    if (filter instanceof Filter) {
                                        return ((Filter) filter).getInput() instanceof TableScan;
                                    }
                                }
                                if (relNode instanceof Project) {
                                    return ((Project) relNode).getInput() instanceof TableScan;
                                }
                            }
                        }
                        return false;
                    }).noInputs())
                    .withDescription("MycatViewIndexViewRule")
                    .as(MycatViewIndexViewRule.Config.class);
        }
    }
}

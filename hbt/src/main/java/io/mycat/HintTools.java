package io.mycat;

import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatMergeSort;
import io.mycat.calcite.rules.MycatJoinRule;
import io.mycat.calcite.rules.MycatMergeJoinRule;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.*;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Define some tool members and methods for hints test.
 */
public class HintTools {
    //~ Static fields/initializers ---------------------------------------------
    static final RelHint JOIN_HINT = RelHint.builder("NO_HASH_JOIN").build();

    public static final HintStrategyTable HINT_STRATEGY_TABLE = createHintStrategies();

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates mock hint strategies.
     *
     * @return HintStrategyTable instance
     */
    public static HintStrategyTable createHintStrategies() {
        return createHintStrategies(HintStrategyTable.builder());
    }

    public static final HintStrategyTable hintStrategyTable = HintStrategyTable.builder()
            .hintStrategy("use_hash_join",
                    HintStrategy.builder(
                            HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName(), (hint, rel) -> {
                                Join join = (Join) rel;
                                JoinInfo info = join.analyzeCondition();
                                return !info.leftKeys.isEmpty()
                                        && !info.rightKeys.isEmpty();
                            }))
                            .converterRules(MycatJoinRule.INSTANCE)
                            .excludedRules(MycatMergeJoinRule.INSTANCE)
                            .build())
            .hintStrategy("use_bka_join",
                    HintStrategy.builder(
                            HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName(), (hint, rel) -> {
                                Join join = (Join) rel;
                                JoinInfo info = join.analyzeCondition();
                                return (join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.LEFT)
                                        && !info.leftKeys.isEmpty()
                                        && !info.rightKeys.isEmpty();
                            }))
                            .converterRules(MycatJoinRule.INSTANCE)
                            .excludedRules(MycatMergeJoinRule.INSTANCE)
                            .build())
            .hintStrategy("use_nl_join",
                    HintStrategy.builder(
                            HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName()))
                            .converterRules(MycatJoinRule.INSTANCE)
                            .excludedRules(MycatMergeJoinRule.INSTANCE)
                            .build())
            .hintStrategy("use_merge_join",
                    HintStrategy.builder(
                            HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName()))
                            .converterRules(MycatMergeJoinRule.INSTANCE)
                            .excludedRules(MycatJoinRule.INSTANCE)
                            .build())
            .hintStrategy("QB_NAME",
                    HintStrategy.builder((hint, rel) -> true)
                            .build())
            .build();

    public static RelHint getLastJoinHint(List<RelHint> hints) {
        if (hints == null) return null;
        return hints.stream().filter(relHint -> {
            switch (relHint.hintName.toLowerCase()) {
                case "no_hash_join":
                case "use_hash_join":
                case "use_bka_join":
                case "use_nl_join":
                case "use_merge_join": {
                    return true;
                }
                default:
                    return false;
            }
        }).findFirst().orElse(null);
    }

    /**
     * Creates mock hint strategies with given builder.
     *
     * @return HintStrategyTable instance
     */
    static HintStrategyTable createHintStrategies(HintStrategyTable.Builder builder) {

        return hintStrategyTable;
    }

    /**
     * Returns a {@link HintPredicate} for join with specified table references.
     */
    private static HintPredicate joinWithFixedTableName() {
        return (hint, rel) -> {
            if (!(rel instanceof LogicalJoin)) {
                return false;
            }
            LogicalJoin join = (LogicalJoin) rel;
            final List<String> tableNames = hint.listOptions;
            if (tableNames.size()!=2){
                return false;
            }

            List<String> leftAlias = new LinkedList<>();
            List<String> rightAlias = new LinkedList<>();
            collectAlias(leftAlias, join.getLeft());
            collectAlias(rightAlias, join.getRight());

            return leftAlias.contains( tableNames.get(0))&&rightAlias.contains(tableNames.get(1));
        };
    }

    private static void collectAlias(List<String> tableNames, RelNode input) {
        final List<RelHint> inputTables = new ArrayList<>();
        HintCollector hintCollector = new HintCollector(inputTables);
        input.accept(hintCollector);
        for (RelHint inputTable : inputTables) {
            if ("QB_NAME".equalsIgnoreCase(inputTable.hintName)) {
                tableNames.addAll((inputTable.listOptions));
            }
        }
        if (input instanceof TableScan) {
            tableNames.add(Util.last(input.getTable().getQualifiedName()));
        }
    }

    private static boolean equalsStringList(List<String> l, List<String> r) {
        if (l.size() != r.size()) {
            return false;
        }
        for (String s : l) {
            if (!r.contains(s)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A shuttle to collect all the hints within the relational expression into a collection.
     */
    private static class HintCollector extends RelShuttleImpl {
        private final List<RelHint> hintsCollect;

        HintCollector(List<RelHint> hintsCollect) {
            this.hintsCollect = hintsCollect;
        }

        @Override
        public RelNode visit(TableScan scan) {
            if (scan.getHints().size() > 0) {
                this.hintsCollect.addAll(scan.getHints());
            }
            return super.visit(scan);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            if (join.getHints().size() > 0) {
                this.hintsCollect.addAll(join.getHints());
            }
            return super.visit(join);
        }

        @Override
        public RelNode visit(LogicalProject project) {
            if (project.getHints().size() > 0) {
                this.hintsCollect.addAll(project.getHints());
            }
            return super.visit(project);
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            if (aggregate.getHints().size() > 0) {
                this.hintsCollect.addAll(aggregate.getHints());
            }
            return super.visit(aggregate);
        }
    }
}
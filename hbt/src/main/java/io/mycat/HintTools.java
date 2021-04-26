package io.mycat;

import io.mycat.calcite.MycatConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.*;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.util.Util;

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
  public static  final   HintStrategyTable hintStrategyTable = HintStrategyTable.builder()
            .hintStrategy("use_hash_join",
                    HintStrategy.builder(
                            HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName())).converterRules().build())
            .hintStrategy("use_bka_join",
                    HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName()))
            .hintStrategy("use_nl_join",
                    HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName()))
            .hintStrategy("use_merge_join",
                    HintStrategy.builder(
                            HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName())).converterRules()
                            .excludedRules(EnumerableRules.ENUMERABLE_JOIN_RULE).build())
            .build();

    public static RelHint getLastJoinHint(List<RelHint> hints){
        if (hints == null)return null;
       return hints.stream().filter(relHint -> {
           switch (relHint.hintName.toLowerCase()){
               case "use_hash_join":
               case "use_bka_join":
               case "use_nl_join":
               case "use_merge_join": {
                   return true;
               }
               default:return false;
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
            final List<String> inputTables = join.getInputs().stream()
                    .filter(input -> input instanceof TableScan)
                    .map(scan -> Util.last(scan.getTable().getQualifiedName()))
                    .collect(Collectors.toList());
            return equalsStringList(tableNames, inputTables);
        };
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
}
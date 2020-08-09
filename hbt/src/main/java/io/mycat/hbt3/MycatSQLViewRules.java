package io.mycat.hbt3;

import org.apache.calcite.plan.RelOptRule;

import java.util.Arrays;
import java.util.List;

public class MycatSQLViewRules {
    public static final List<RelOptRule> RULES = Arrays.asList(
            MycatFilterViewRule.INSTANCE,
            MycatProjectViewRule.INSTANCE,
            MycatJoinViewRule.INSTANCE,
            MycatAggregateViewRule.INSTANCE,
            MycatSortViewRule.INSTANCE
            );
}
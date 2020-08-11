package io.mycat.hbt3;

import org.apache.calcite.plan.RelOptRule;

import java.util.Arrays;
import java.util.List;

public class MycatGatherRules {
    public static final RelOptRule CREATE_VIEW_RULE = MycatGatherCreateRule.ON_VIEW;
    public static final RelOptRule GATHER_REMOVE_RULE =  MycatGatherRemoveRule.INSTANCE;


    public static final List<RelOptRule> RULES = Arrays.asList(
//            CREATE_VIEW_RULE,
            GATHER_REMOVE_RULE
            );

}
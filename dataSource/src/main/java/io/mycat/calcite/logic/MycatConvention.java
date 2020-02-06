package io.mycat.calcite.logic;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.sql.SqlDialect;

public class MycatConvention extends Convention.Impl {


    public final SqlDialect dialect;
    public final String targetName;

    public MycatConvention(String targetName,SqlDialect dialect) {
        super("MYCAT2."+targetName, MycatRel.class);
        this.dialect = dialect;
        this.targetName = targetName;
    }

    public static MycatConvention of(String targetName,SqlDialect dialect) {
        return new MycatConvention(targetName,dialect);
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(FilterSetOpTransposeRule.INSTANCE);
        planner.addRule(ProjectRemoveRule.INSTANCE);
    }
}

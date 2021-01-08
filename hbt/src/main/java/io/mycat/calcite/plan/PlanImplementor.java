package io.mycat.calcite.plan;

import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.spm.Plan;

public interface PlanImplementor {
    void execute(MycatUpdateRel logical);

    void execute(MycatInsertRel logical);

    void execute(Plan plan);
}

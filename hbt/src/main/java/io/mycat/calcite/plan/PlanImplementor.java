package io.mycat.calcite.plan;

import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.spm.Plan;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;

public interface PlanImplementor {
    Future<Void> execute(MycatUpdateRel logical);

    Future<Void>  execute(MycatInsertRel logical);

    Future<Void> execute(Plan plan);
}

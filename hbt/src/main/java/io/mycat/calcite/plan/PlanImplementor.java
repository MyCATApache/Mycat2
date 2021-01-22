package io.mycat.calcite.plan;

import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.spm.Plan;
import io.vertx.core.impl.future.PromiseInternal;

public interface PlanImplementor {
    PromiseInternal<Void>  execute(MycatUpdateRel logical);

    PromiseInternal<Void>  execute(MycatInsertRel logical);

    PromiseInternal<Void> execute(Plan plan);
}

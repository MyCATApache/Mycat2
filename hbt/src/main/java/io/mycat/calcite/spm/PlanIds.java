package io.mycat.calcite.spm;

import com.imadcn.framework.idworker.algorithm.Snowflake;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.ServerConfig;
import lombok.SneakyThrows;

import java.util.concurrent.atomic.AtomicLong;

public class PlanIds {
    private final Snowflake PLAN_IDS;
    private final Snowflake BASELINE_IDS;

    public PlanIds() {
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
        long mycatId = serverConfig.getMycatId();

        this.PLAN_IDS = Snowflake.create(mycatId);
        this.BASELINE_IDS = Snowflake.create(mycatId);
    }

    @SneakyThrows
    long nextBaselineId() {
        return this.BASELINE_IDS.nextId();
    }

    @SneakyThrows
    long nextPlanId() {
        return this.PLAN_IDS.nextId();
    }
}

package io.mycat.calcite.spm;

import io.mycat.MetaClusterCurrent;
import io.mycat.config.ServerConfig;
import lombok.SneakyThrows;

import java.util.concurrent.atomic.AtomicLong;

public class PlanIds {
   private final AtomicLong PLAN_IDS;
    private final AtomicLong BASELINE_IDS;

    public PlanIds() {
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
        int mycatId = serverConfig.getMycatId();
        this.PLAN_IDS = new AtomicLong(mycatId + System.currentTimeMillis() >> 4);
        this.BASELINE_IDS = new AtomicLong(mycatId + System.currentTimeMillis() >> 4);
    }

    @SneakyThrows
    long nextBaselineId() {
        return this.BASELINE_IDS.getAndIncrement();
    }

    @SneakyThrows
    long nextPlanId() {
        return BASELINE_IDS.getAndIncrement();
    }
}

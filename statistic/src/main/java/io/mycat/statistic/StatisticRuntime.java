package io.mycat.statistic;

import io.mycat.MycatConfig;

import java.util.Objects;

public enum  StatisticRuntime {
    INSTANCE;
    private volatile MycatConfig config;
    public synchronized void load(MycatConfig config) {
        Objects.requireNonNull(config);
        if (this.config == config) {
            return;
        }
        innerThis(config);
        this.config = config;
    }

    private void innerThis(MycatConfig config) {
        MetadataManager
    }
}
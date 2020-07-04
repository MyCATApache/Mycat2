package io.mycat.util;

import io.mycat.ScheduleUtil;

import java.util.concurrent.TimeUnit;

public enum TimeProvider {
    INSTANCE;
    private volatile long now;

    private TimeProvider() {
        this.now = System.currentTimeMillis();
        scheduleTick();
    }

    private void scheduleTick() {
        ScheduleUtil.getTimer().scheduleAtFixedRate(() -> {
            now = System.currentTimeMillis();
        }, 1, 1, TimeUnit.MILLISECONDS);
    }

    public long now() {
        return now;
    }
}
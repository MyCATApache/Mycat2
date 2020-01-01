package io.mycat;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ScheduleUtil {
    final static ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);

    public static ScheduledExecutorService getTimer() {
        return timer;
    }

}
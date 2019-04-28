package io.mycat.proxy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MycatScheduler {
    final AtomicLong sessionIdCounter = new AtomicLong(0);

    public long scheduleAtFixedRate(Runnable command, long period, TimeUnit unit) {
        long id = sessionIdCounter.getAndIncrement();
        return id;
    }

    public void cancel(long id){

    }

}

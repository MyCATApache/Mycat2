package io.mycat.beans.log.monitor;

import com.google.common.util.concurrent.AtomicDouble;

import java.util.concurrent.atomic.AtomicLong;

public class InstanceMonitor {

    private static final AtomicLong lqpsCount = new AtomicLong();
    private static final AtomicLong pqpsCount = new AtomicLong();

    private static final AtomicLong lrt = new AtomicLong();
    private static final AtomicLong prt = new AtomicLong();


    public static final void plusLrt(long value) {
        lrt.getAndAdd(value);
        lqpsCount.getAndAdd(1);
    }

    public static final void plusPrt(long value) {
        prt.getAndAdd(value);
        pqpsCount.getAndIncrement();
    }

    public void reset() {
        lqpsCount.set(0);
        pqpsCount.set(0);

        lrt.set(0);
        prt.set(0);
    }

    public static double getLqps() {
        return lrt.get()*1.0/ lqpsCount.get();
    }

    public static double getPqps() {
        return prt.get()*1.0/pqpsCount.get();
    }

    public static long getLrt() {
        return lrt.get();
    }

    public static double getPrt() {
        return prt.get();
    }
}

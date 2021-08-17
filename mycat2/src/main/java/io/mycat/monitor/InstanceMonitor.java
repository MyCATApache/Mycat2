package io.mycat.monitor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class InstanceMonitor {

    private static final AtomicLong lqpsCount = new AtomicLong();
    private static final AtomicLong pqpsCount = new AtomicLong();

    private static final AtomicLong lrt = new AtomicLong();
    private static final AtomicLong prt = new AtomicLong();


    private static long startTime = System.currentTimeMillis();

    public static final void plusLrt(long value) {
        lrt.getAndAdd(value);
        lqpsCount.getAndAdd(1);
    }

    public static final void plusPrt(long value) {
        prt.getAndAdd(value);
        pqpsCount.getAndIncrement();
    }

    public static void reset() {
        lqpsCount.set(0);
        pqpsCount.set(0);

        lrt.set(0);
        prt.set(0);

        startTime = System.currentTimeMillis();
    }

    public static double getLqps() {
       return  ((long)(lqpsCount.get() * 1.0/ getSecondTIme()));
    }

    private static long getSecondTIme() {
        return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
    }

    public static double getPqps() {
        return ((long)(pqpsCount.get() * 1.0 / getSecondTIme()));
    }

    public static double getLrt() {
        if (lqpsCount.get() == 0){
            return 0;
        }
        return  ((long)(lrt.get() * 1.0 / lqpsCount.get()));
    }

    public static double getPrt() {
        if (pqpsCount.get() == 0){
            return 0;
        }
        return ((long)(prt.get() * 1.0 / pqpsCount.get()));
    }
}

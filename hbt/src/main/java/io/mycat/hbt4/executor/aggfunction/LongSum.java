package io.mycat.hbt4.executor.aggfunction;

/**
 * Implementation of {@code SUM} over BIGINT values as a user-defined
 * aggregate.
 */
public class LongSum {
    public LongSum() {
    }

    public long init() {
        return 0L;
    }

    public long add(long accumulator, long v) {
        return accumulator + v;
    }

    public long merge(long accumulator0, long accumulator1) {
        return accumulator0 + accumulator1;
    }

    public long result(long accumulator) {
        return accumulator;
    }
}

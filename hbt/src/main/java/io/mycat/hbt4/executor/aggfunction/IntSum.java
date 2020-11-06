package io.mycat.hbt4.executor.aggfunction;

/**
 * Implementation of {@code SUM} over INTEGER values as a user-defined
 * aggregate.
 */
public class IntSum {
    public IntSum() {
    }

    public int init() {
        return 0;
    }

    public int add(int accumulator, int v) {
        return accumulator + v;
    }

    public int merge(int accumulator0, int accumulator1) {
        return accumulator0 + accumulator1;
    }

    public int result(int accumulator) {
        return accumulator;
    }
}
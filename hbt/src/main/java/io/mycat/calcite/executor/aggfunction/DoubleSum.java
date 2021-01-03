package io.mycat.calcite.executor.aggfunction;

/**
 * Implementation of {@code SUM} over DOUBLE values as a user-defined
 * aggregate.
 */
public class DoubleSum {
    public DoubleSum() {
    }

    public double init() {
        return 0D;
    }

    public double add(double accumulator, double v) {
        return accumulator + v;
    }

    public double merge(double accumulator0, double accumulator1) {
        return accumulator0 + accumulator1;
    }

    public double result(double accumulator) {
        return accumulator;
    }
}

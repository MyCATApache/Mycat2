package io.mycat.calcite.executor.aggfunction;

/**
 * Implementation of {@code MIN} function to calculate the minimum of
 * {@code boolean} values as a user-defined aggregate.
 */
public class MinBoolean {
    public MinBoolean() {
    }

    public Boolean init() {
        return Boolean.TRUE;
    }

    public Boolean add(Boolean accumulator, Boolean value) {
        return accumulator.compareTo(value) < 0 ? accumulator : value;
    }

    public Boolean merge(Boolean accumulator0, Boolean accumulator1) {
        return add(accumulator0, accumulator1);
    }

    public Boolean result(Boolean accumulator) {
        return accumulator;
    }
}

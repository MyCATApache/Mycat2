package io.mycat.calcite.executor.aggfunction;

import io.mycat.mpp.Row;

/**
 * Accumulator that applies a filter to another accumulator.
 * The filter is a BOOLEAN field in the input row.
 */
public class FilterAccumulator implements Accumulator {
    private final Accumulator accumulator;
    private final int filterArg;

    FilterAccumulator(Accumulator accumulator, int filterArg) {
        this.accumulator = accumulator;
        this.filterArg = filterArg;
    }

    public void send(Row row) {
        if (row.getValues()[filterArg] == Boolean.TRUE) {
            accumulator.send(row);
        }
    }

    public Object end() {
        return accumulator.end();
    }
}
    
package io.mycat.calcite.executor.aggfunction;

import java.util.function.BiFunction;

/**
 * Common implementation of comparison aggregate methods over numeric
 * values as a user-defined aggregate.
 *
 * @param <T> The numeric type
 */
public class NumericComparison<T> {
    private final T initialValue;
    private final BiFunction<T, T, T> comparisonFunction;

    public NumericComparison(T initialValue, BiFunction<T, T, T> comparisonFunction) {
        this.initialValue = initialValue;
        this.comparisonFunction = comparisonFunction;
    }

    public T init() {
        return this.initialValue;
    }

    public T add(T accumulator, T value) {
        return this.comparisonFunction.apply(accumulator, value);
    }

    public T merge(T accumulator0, T accumulator1) {
        return add(accumulator0, accumulator1);
    }

    public T result(T accumulator) {
        return accumulator;
    }
}
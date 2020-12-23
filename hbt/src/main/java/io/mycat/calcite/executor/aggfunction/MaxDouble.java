package io.mycat.calcite.executor.aggfunction;

/**
 * Implementation of {@code MAX} function to calculate the maximum of
 * {@code double} and {@code real} values as a user-defined aggregate.
 */
public class MaxDouble extends NumericComparison<Double> {
    public MaxDouble() {
        super(Double.MIN_VALUE, Math::max);
    }
}
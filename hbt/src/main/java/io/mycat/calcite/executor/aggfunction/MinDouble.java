package io.mycat.calcite.executor.aggfunction;

/**
     * Implementation of {@code MIN} function to calculate the minimum of
     * {@code double} and {@code real} values as a user-defined aggregate.
     */
    public  class MinDouble extends NumericComparison<Double> {
        public MinDouble() {
            super(Double.MAX_VALUE, Math::min);
        }
    }

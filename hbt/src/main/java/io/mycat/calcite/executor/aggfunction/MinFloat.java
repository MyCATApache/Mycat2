package io.mycat.calcite.executor.aggfunction;

/**
     * Implementation of {@code MIN} function to calculate the minimum of
     * {@code float} values as a user-defined aggregate.
     */
    public  class MinFloat extends NumericComparison<Float> {
        public MinFloat() {
            super(Float.MAX_VALUE, Math::min);
        }
    }
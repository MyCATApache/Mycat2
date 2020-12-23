package io.mycat.calcite.executor.aggfunction;

/**
     * Implementation of {@code MIN} function to calculate the minimum of
     * {@code integer} values as a user-defined aggregate.
     */
    public  class MinInt extends NumericComparison<Integer> {
        public MinInt() {
            super(Integer.MAX_VALUE, Math::min);
        }
    }
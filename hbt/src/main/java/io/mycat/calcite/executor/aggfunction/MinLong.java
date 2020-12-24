package io.mycat.calcite.executor.aggfunction;

/**
     * Implementation of {@code MIN} function to calculate the minimum of
     * {@code long} values as a user-defined aggregate.
     */
    public  class MinLong extends NumericComparison<Long> {
        public MinLong() {
            super(Long.MAX_VALUE, Math::min);
        }
    }

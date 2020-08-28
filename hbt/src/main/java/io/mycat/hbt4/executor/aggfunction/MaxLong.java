package io.mycat.hbt4.executor.aggfunction;

/**
     * Implementation of {@code MAX} function to calculate the maximum of
     * {@code long} values as a user-defined aggregate.
     */
    public  class MaxLong extends NumericComparison<Long> {
        public MaxLong() {
            super(Long.MIN_VALUE, Math::max);
        }
    }
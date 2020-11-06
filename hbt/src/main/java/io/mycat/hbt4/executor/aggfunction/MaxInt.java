package io.mycat.hbt4.executor.aggfunction;

/**
     * Implementation of {@code MAX} function to calculate the maximum of
     * {@code integer} values as a user-defined aggregate.
     */
    public  class MaxInt extends NumericComparison<Integer> {
        public MaxInt() {
            super(Integer.MIN_VALUE, Math::max);
        }
    }

package io.mycat.hbt4.executor.aggfunction;

/**
     * Implementation of {@code MAX} function to calculate the maximum of
     * {@code float} values as a user-defined aggregate.
     */
    public  class MaxFloat extends NumericComparison<Float> {
        public MaxFloat() {
            super(Float.MIN_VALUE, Math::max);
        }
    }

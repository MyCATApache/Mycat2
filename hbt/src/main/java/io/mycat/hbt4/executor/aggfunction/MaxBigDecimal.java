package io.mycat.hbt4.executor.aggfunction;

import java.math.BigDecimal;

/**
     * Implementation of {@code MAX} function to calculate the maximum of
     * {@code BigDecimal} values as a user-defined aggregate.
     */
    public  class MaxBigDecimal extends NumericComparison<BigDecimal> {
        public MaxBigDecimal() {
            super(new BigDecimal(Double.MIN_VALUE), MaxBigDecimal::max);
        }

        public static BigDecimal max(BigDecimal a, BigDecimal b) {
            return a.max(b);
        }
    }
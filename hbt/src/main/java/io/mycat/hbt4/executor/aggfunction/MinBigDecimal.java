package io.mycat.hbt4.executor.aggfunction;

import java.math.BigDecimal;

/**
     * Implementation of {@code MIN} function to calculate the minimum of
     * {@code BigDecimal} values as a user-defined aggregate.
     */
    public  class MinBigDecimal extends NumericComparison<BigDecimal> {
        public MinBigDecimal() {
            super(new BigDecimal(Double.MAX_VALUE), MinBigDecimal::min);
        }

        public static BigDecimal min(BigDecimal a, BigDecimal b) {
            return a.min(b);
        }
    }
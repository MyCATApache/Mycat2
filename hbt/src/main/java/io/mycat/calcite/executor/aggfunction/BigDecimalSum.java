package io.mycat.calcite.executor.aggfunction;

import java.math.BigDecimal;

/**
     * Implementation of {@code SUM} over BigDecimal values as a user-defined
     * aggregate.
     */
    public  class BigDecimalSum {
        public BigDecimalSum() {
        }

        public BigDecimal init() {
            return new BigDecimal("0");
        }

        public BigDecimal add(BigDecimal accumulator, BigDecimal v) {
            return accumulator.add(v);
        }

        public BigDecimal merge(BigDecimal accumulator0, BigDecimal accumulator01) {
            return add(accumulator0, accumulator01);
        }

        public BigDecimal result(BigDecimal accumulator) {
            return accumulator;
        }
    }

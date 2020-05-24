package io.mycat.mpp;

import java.math.BigDecimal;

public class Avg implements AggCalls.AggCall {
    long count = 0;
    long sum = 0;

    @Override
    public String name() {
        return "avg";
    }

    @Override
    public void accept(Object value) {
        count += 1;
        sum += ((Number) value).longValue();
    }

    @Override
    public BigDecimal getValue() {
        return BigDecimal.valueOf(count).divide(BigDecimal.valueOf(sum));
    }

    @Override
    public void reset() {
        count = 0;
        sum = 0;
    }

    @Override
    public Class type() {
        return BigDecimal.class;
    }

    @Override
    public void merge(AggCalls.AggCall call) {
        Avg avg = (Avg) call;
        count += avg.count;
        sum += avg.sum;
    }
}
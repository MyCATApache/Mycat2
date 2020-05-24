package io.mycat.mpp;

public class Sum implements AggCalls.AggCall {
    long sum = 0;

    @Override
    public String name() {
        return "sum";
    }

    @Override
    public void accept(Object value) {
        sum+=((Number)value).longValue();
    }

    @Override
    public Long getValue() {
        return sum;
    }

    @Override
    public void reset() {
        sum = 0;
    }

    @Override
    public void merge(AggCalls.AggCall call) {
        Sum call1 = (Sum) call;
        sum+=call1.sum;
    }

}
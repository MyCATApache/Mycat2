package io.mycat.mpp;

public class Sum implements AggCalls.AggCall {
    long sum = 0;
    @Override
    public void accept(long value) {
        sum+=value;
    }

    @Override
    public long getValue() {
        return sum;
    }

    @Override
    public void reset() {
        sum = 0;
    }

}
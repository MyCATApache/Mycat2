package io.mycat.mpp;

public class Count implements AggCalls.AggCall {
    long count = 0;

    @Override
    public void accept(long value) {
        count += value;
    }

    @Override
    public long getValue() {
        return count;
    }

    @Override
    public void reset() {
        count = 0;
    }
}
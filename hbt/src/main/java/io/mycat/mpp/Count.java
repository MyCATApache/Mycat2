package io.mycat.mpp;

public class Count implements AggCalls.AggCall {
    long count = 0;

    @Override
    public String name() {
        return "count";
    }

    @Override
    public void accept(Object value) {
        count += 1;
    }

    @Override
    public Long getValue() {
        return count;
    }

    @Override
    public void reset() {
        count = 0;
    }

    @Override
    public void merge(AggCalls.AggCall call) {
        count+= ((Count)call).count;
    }
}
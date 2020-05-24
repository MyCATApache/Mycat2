package io.mycat.mpp;

public class Avg implements AggCalls.AggCall {
    long count = 0;
    long sum = 0;
    @Override
    public void accept(long value) {
        count+=1;
        sum+=value;
    }

    @Override
    public long getValue() {
        return sum/count;
    }

    @Override
    public void reset() {

    }
}
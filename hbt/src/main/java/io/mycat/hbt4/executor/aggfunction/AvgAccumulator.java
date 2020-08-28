package io.mycat.hbt4.executor.aggfunction;

import io.mycat.mpp.Row;
import org.apache.calcite.rel.core.AggregateCall;

/**
 * Accumulator for calls to the COUNT function.
 */
public class AvgAccumulator implements Accumulator {
    private final AggregateCall call;
    long cnt;
    double sum;

    AvgAccumulator(AggregateCall call) {
        this.call = call;
        cnt = 0;
        sum = 0;
    }

    public void send(Row row) {
        boolean notNull = true;
        Integer integer = call.getArgList().get(0);
        Number object = (Number) row.getObject(integer);
        if (object == null) {
            notNull = false;
        }
        if (notNull) {
            cnt++;
            sum += object.doubleValue();
        }
    }

    public Object end() {
        return sum / cnt;
    }
}

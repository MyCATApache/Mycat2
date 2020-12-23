package io.mycat.calcite.executor.aggfunction;

import io.mycat.mpp.Row;
import org.apache.calcite.rel.core.AggregateCall;

/**
 * Accumulator for calls to the COUNT function.
 */
public class CountAccumulator implements Accumulator {
    private final AggregateCall call;
    long cnt;

    CountAccumulator(AggregateCall call) {
        this.call = call;
        cnt = 0;
    }

    public void send(Row row) {
        boolean notNull = true;
        for (Integer i : call.getArgList()) {
            if (row.getObject(i) == null) {
                notNull = false;
                break;
            }
        }
        if (notNull) {
            cnt++;
        }
    }

    public Object end() {
        return cnt;
    }
}

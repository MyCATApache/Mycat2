package io.mycat.hbt4.executor.aggfunction;

import io.mycat.mpp.Row;

import java.util.ArrayList;

/**
 * A list of accumulators used during grouping.
 */
public class AccumulatorList extends ArrayList<Accumulator> {
    public void send(Row row) {
        for (Accumulator a : this) {
            a.send(row);
        }
    }

    public void end(Row r) {
        for (int accIndex = 0, rowIndex = r.size() - size();
             rowIndex < r.size(); rowIndex++, accIndex++) {
            r.set(rowIndex, get(accIndex).end());
        }
    }
}
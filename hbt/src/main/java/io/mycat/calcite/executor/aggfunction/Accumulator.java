package io.mycat.calcite.executor.aggfunction;

import io.mycat.mpp.Row;

/**
 * Defines function implementation for
 * things like {@code count()} and {@code sum()}.
 */
public interface Accumulator {
    void send(Row row);

    Object end();
}
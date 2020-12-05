package io.mycat.hbt4.executor.aggfunction;

import org.apache.calcite.MycatContext;
import io.mycat.hbt4.executor.MycatScalar;
import lombok.SneakyThrows;
import org.objenesis.instantiator.util.UnsafeUtils;

/**
 * Accumulator powered by {@link MycatScalar} code fragments.
 */
public class ScalarAccumulatorDef implements AccumulatorFactory {
    final MycatScalar initScalar;
    final MycatScalar addScalar;
    final MycatScalar endScalar;
    final MycatContext sendContext;
    final MycatContext endContext;
    final int rowLength;
    final int accumulatorLength;

    @SneakyThrows
    private ScalarAccumulatorDef(MycatScalar initScalar, MycatScalar addScalar,
                                 MycatScalar endScalar, int rowLength, int accumulatorLength) {
        this.initScalar = initScalar;
        this.addScalar = addScalar;
        this.endScalar = endScalar;
        this.accumulatorLength = accumulatorLength;
        this.rowLength = rowLength;
        this.sendContext = new MycatContext();
        this.sendContext.values = new Object[rowLength + accumulatorLength];
        this.endContext = new MycatContext();
        this.endContext.values = new Object[accumulatorLength];
    }

    public Accumulator get() {
        return new ScalarAccumulator(this, new Object[accumulatorLength]);
    }
}
package io.mycat.calcite.executor.aggfunction;

import java.util.function.Supplier;

/**
 * Creates an {@link Accumulator}.
 */
public interface AccumulatorFactory extends Supplier<Accumulator> {
}

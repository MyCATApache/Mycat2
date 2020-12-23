package io.mycat.calcite.executor.aggfunction;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Accumulator factory based on a user-defined aggregate function.
 */
public class UdaAccumulatorFactory implements AccumulatorFactory {
    final AggregateFunctionImpl aggFunction;
    final int argOrdinal;
    public final Object instance;
    public final boolean nullIfEmpty;

    UdaAccumulatorFactory(AggregateFunctionImpl aggFunction,
                          AggregateCall call, boolean nullIfEmpty) {
        this.aggFunction = aggFunction;
        if (call.getArgList().size() != 1) {
            throw new UnsupportedOperationException("in current implementation, "
                    + "aggregate must have precisely one argument");
        }
        argOrdinal = call.getArgList().get(0);
        if (aggFunction.isStatic) {
            instance = null;
        } else {
            try {
                final Constructor<?> constructor =
                        aggFunction.declaringClass.getConstructor();
                instance = constructor.newInstance();
            } catch (InstantiationException | IllegalAccessException
                    | NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        this.nullIfEmpty = nullIfEmpty;
    }

    public Accumulator get() {
        return new UdaAccumulator(this);
    }
}

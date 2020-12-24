package io.mycat.calcite.executor.aggfunction;

import io.mycat.mpp.Row;

import java.lang.reflect.InvocationTargetException;

/**
     * Accumulator based upon a user-defined aggregate.
     */
    public  class UdaAccumulator implements Accumulator {
        private final UdaAccumulatorFactory factory;
        private Object value;
        private boolean empty;

        UdaAccumulator(UdaAccumulatorFactory factory) {
            this.factory = factory;
            try {
                this.value = factory.aggFunction.initMethod.invoke(factory.instance);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            this.empty = true;
        }

        public void send(Row row) {
            final Object[] args = {value, row.getValues()[factory.argOrdinal]};
            for (int i = 1; i < args.length; i++) {
                if (args[i] == null) {
                    return; // one of the arguments is null; don't add to the total
                }
            }
            try {
                value = factory.aggFunction.addMethod.invoke(factory.instance, args);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            empty = false;
        }

        public Object end() {
            if (factory.nullIfEmpty && empty) {
                return null;
            }
            final Object[] args = {value};
            try {
                return factory.aggFunction.resultMethod.invoke(factory.instance, args);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }
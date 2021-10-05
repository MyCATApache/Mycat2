package io.ordinate.engine.function.cast;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.function.AbstractUnaryLongFunction;

import java.util.List;

public class CastDatetimeToLongFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(datetime):int64";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }

    private static class Func extends AbstractUnaryLongFunction {
        boolean isNull;
        public Func(Function arg) {
            super(arg);
        }

        @Override
        public long getLong(Record rec) {
            long datetime = arg.getDatetime(rec);
            isNull= arg.isNull(rec);
            if (isNull)return 0;
            return datetime;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}

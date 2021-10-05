package io.ordinate.engine.function.cast;


import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.UnaryFunction;
import io.ordinate.engine.function.Numbers;
import io.ordinate.engine.record.Record;


import java.util.List;

public class CastDatetimeToIntFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(datetime):int32";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }
    private static class Func extends IntFunction implements UnaryFunction {
        private final Function arg;
        boolean isNull;

        public Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getDatetime(rec);
            isNull = arg.isNull(rec);
            if (isNull)return 0;
            return  (int) value;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }

}

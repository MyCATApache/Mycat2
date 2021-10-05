package io.ordinate.engine.function.cast;


import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.function.UnaryFunction;
import io.ordinate.engine.function.Numbers;
import io.ordinate.engine.record.Record;


import java.sql.Timestamp;
import java.util.List;

public class CastDatetimeToStringFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(datetime):string";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }
    private static class Func extends StringFunction implements UnaryFunction {
        private final Function arg;
        private boolean isNull;

        public Func(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getString(Record rec) {
            final long value = arg.getDatetime(rec);
            isNull = arg.isNull(rec);
            if (isNull)return null;
            return new Timestamp(value).toString();
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}

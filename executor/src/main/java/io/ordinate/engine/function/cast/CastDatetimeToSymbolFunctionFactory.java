package io.ordinate.engine.function.cast;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.SymbolFunction;
import io.ordinate.engine.function.UnaryFunction;
import io.ordinate.engine.function.Numbers;
import io.ordinate.engine.record.Record;


import java.sql.Timestamp;
import java.util.List;

public class CastDatetimeToSymbolFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cast(datetime):symbol";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }

    private static class Func extends SymbolFunction implements UnaryFunction {
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
        public CharSequence getSymbol(Record rec) {
            final long value = arg.getDatetime(rec);
            isNull = arg.isNull(rec);
            if (isNull) return null;
            return new Timestamp(value).toString();
        }

        @Override
        public int getInt(Record rec) {
            final long value = arg.getDatetime(rec);
            isNull = arg.isNull(rec);
            if (isNull) return 0;
            return (int) value;
        }
    }
}

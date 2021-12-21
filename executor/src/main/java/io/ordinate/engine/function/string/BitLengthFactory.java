package io.ordinate.engine.function.string;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;

public class BitLengthFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "BIT_LENGTH(string)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new BitLengthFactory.Func(args);
    }

    private static final class Func extends IntFunction {

        private final List<Function> args;
        private boolean isNull;

        public Func(List<Function> args) {
            super();
            this.args = args;
        }

        @Override
        public List<Function> getArgs() {
            return args;
        }

        @Override
        public int getInt(Record rec) {
            Function function = args.get(0);
            CharSequence sequence = function.getString(rec);
            isNull = function.isNull(rec);
            if (isNull) return 0;
            return sequence.length() * 8;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}

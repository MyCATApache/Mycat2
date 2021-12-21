package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.CharFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.ArrayList;
import java.util.List;

public class CharFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "char(string)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new CharFactory.Func(args);
    }

    private static final class Func extends StringFunction {

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
        public CharSequence getString(Record rec) {
            List<CharSequence> charSequences = new ArrayList<>();
            for (Function arg : args) {
                charSequences.add(arg.getString(rec));
            }
            String s = CharFunction.charFunction(charSequences.toArray(new Object[]{}));
            isNull = s == null;
            if (isNull) return null;
            return s;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}

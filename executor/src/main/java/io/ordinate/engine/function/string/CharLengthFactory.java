package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.CharLengthFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.record.Record;

import java.util.List;

public class CharLengthFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "char_length(string)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new CharLengthFactory.Func(args);
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
            CharSequence string = function.getString(rec);
            isNull = string == null;
            if (!isNull) {
                return CharLengthFunction.charLength(string.toString());
            }
            return 0;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}

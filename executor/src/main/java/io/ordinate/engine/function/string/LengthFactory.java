package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.LengthFunction;
import io.mycat.calcite.sqlfunction.stringfunction.ToBase64Function;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.Collections;
import java.util.List;

public class LengthFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "length(string)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }

    private static final class Func extends IntFunction {

        private final Function arg;
        private boolean isNull;

        public Func(Function arg) {
            super();
            this.arg = arg;
        }

        @Override
        public List<Function> getArgs() {
            return Collections.singletonList(arg);
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }

        @Override
        public int getInt(Record rec) {
            CharSequence value = arg.getString(rec);
            isNull = arg.isNull(rec);
            if (isNull) return 0;
            return LengthFunction.length(value.toString());
        }
    }
}

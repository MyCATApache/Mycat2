package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.HexFunction;
import io.mycat.calcite.sqlfunction.stringfunction.ToBase64Function;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.Collections;
import java.util.List;

public class HexFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "hex()";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args.get(0));
    }

    private static final class Func extends StringFunction {

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
        public CharSequence getString(Record rec) {
            Object value = arg.getAsObject(rec);
            isNull = arg.isNull(rec);
            if (isNull) return null;
            if (value instanceof BinarySequence) {
                value = ((BinarySequence) value).getBytes();
            }
            return HexFunction.hex(value);
        }
    }
}

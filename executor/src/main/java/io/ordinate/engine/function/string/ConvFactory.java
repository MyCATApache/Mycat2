package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.ConvFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;

public class ConvFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "conv(string,int,int)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new ConvFactory.Func(args);
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
            Function one = args.get(0);
            Function two = args.get(1);
            Function three = args.get(2);
            isNull = one.isNull(rec) || two.isNull(rec) || three.isNull(rec);
            if (isNull) return null;
            String conv = ConvFunction.conv(one.getString(rec).toString(), two.getInt(rec), three.getInt(rec));
            isNull = conv == null;
            if (isNull) return null;
            return conv;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}

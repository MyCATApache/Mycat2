package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.EltFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.ArrayList;
import java.util.List;

public class EltFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "elt()";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new EltFactory.Func(args);
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
            int anInt = args.get(0).getInt(rec);

            List<String> charSequences = new ArrayList<>();
            for (Function arg : args.subList(1, args.size())) {
                isNull = arg.isNull(rec);
                if (isNull) return null;
                charSequences.add(arg.getString(rec).toString());
            }
            String s = EltFunction.elt(anInt, charSequences.toArray(new String[]{}));
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

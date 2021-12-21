package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.FindInSetFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.record.Record;

import java.util.ArrayList;
import java.util.List;

public class FindInSetFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "FIND_IN_SET(string,string)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
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
            List<String> charSequences = new ArrayList<>();
            for (Function arg : args) {
                isNull = arg.isNull(rec);
                if (isNull) return 0;
                charSequences.add(arg.getString(rec).toString());
            }
            Integer integer = FindInSetFunction.findInSet(charSequences.get(0), charSequences.get(1));
            isNull = integer == null;
            if (isNull) return 0;
            return integer;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}

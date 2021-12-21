package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.InstrFunction;
import io.mycat.calcite.sqlfunction.stringfunction.SubStringFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;
import java.util.stream.Collectors;

public class SubStringFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "substring()";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new SubStringFactory.Func(args);
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
        public String getString(Record rec) {
            Object[] objects = args.stream().map(i -> i.getAsObject(rec)).toArray();
            String subString = SubStringFunction.subString(objects);
            isNull = subString == null;
            if (isNull) return null;
            return subString;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}

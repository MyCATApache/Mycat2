package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.LtrimFunction;
import io.mycat.calcite.sqlfunction.stringfunction.MakeSetFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;
import java.util.stream.Collectors;

public class MakeSetFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "MAKE_SET()";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
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
        public boolean isNull(Record rec) {
            return isNull;
        }

        @Override
        public String getString(Record rec) {
            List<Object> collect = this.args.stream().map(i -> i.getAsObject(rec)).collect(Collectors.toList());
            String makeSet = MakeSetFunction.makeSet((Number) collect.get(0),collect.subList(1,collect.size()).toArray(new String[]{}));
            isNull = makeSet == null;
            if (isNull) return null;
            return makeSet;
        }
    }
}

package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.MakeSetFunction;
import io.mycat.calcite.sqlfunction.stringfunction.StringIndexFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;
import java.util.stream.Collectors;

public class SubStringIndexFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "SUBSTRING_INDEX(string,string,int)";
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
            Function one = this.args.get(0);
            Function two = this.args.get(1);
            Function three = this.args.get(2);
            isNull = one.isNull(rec) || two.isNull(rec) || three.isNull(rec);
            if (isNull) return null;
            String subStringIndex = StringIndexFunction.subStringIndex(one.getString(rec).toString(), two.getString(rec).toString(), three.getInt(rec));
            isNull = subStringIndex == null;
            if (isNull) return null;
            return subStringIndex;
        }
    }
}

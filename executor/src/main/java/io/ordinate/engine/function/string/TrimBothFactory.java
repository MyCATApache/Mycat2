package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.TrimBothFunction;
import io.mycat.calcite.sqlfunction.stringfunction.TrimLeadingFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.util.List;

public class TrimBothFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "TRIM_BOTH(string,string)";
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
            isNull = one.isNull(rec) || two.isNull(rec);
            if (isNull) return null;
            String trimLeading = TrimBothFunction.trim_both(one.getString(rec).toString(), two.getString(rec).toString());
            isNull = trimLeading == null;
            if (isNull) return null;
            return trimLeading;
        }
    }
}

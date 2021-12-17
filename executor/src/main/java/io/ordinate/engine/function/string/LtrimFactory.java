package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.LpadFunction;
import io.mycat.calcite.sqlfunction.stringfunction.LtrimFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.function.constant.StringConstant;
import io.ordinate.engine.record.Record;

import java.util.List;

public class LtrimFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "ltrim(string)";
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
            isNull = one.isNull(rec);
            if (isNull) return null;
            String ltrim = LtrimFunction.ltrim(one.getString(rec).toString());
            isNull = ltrim == null;
            if (isNull) return null;
            return ltrim;
        }
    }
}

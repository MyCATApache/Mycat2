package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.LocateFunction;
import io.mycat.calcite.sqlfunction.stringfunction.LpadFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.function.constant.IntConstant;
import io.ordinate.engine.function.constant.StringConstant;
import io.ordinate.engine.record.Record;

import java.util.List;

public class LpadFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "lpad()";
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
            Function three;
            if (this.args.size() > 2) {
                three = this.args.get(2);
            } else {
                three = StringConstant.newInstance(" ");
            }
            isNull = one.isNull(rec) || two.isNull(rec) || three.isNull(rec);
            if (isNull) return null;
            String lpad = LpadFunction.lpad(one.getString(rec).toString(), two.getInt(rec), three.getString(rec).toString());
            isNull = lpad == null;
            if (isNull) return null;
            return lpad;
        }
    }
}

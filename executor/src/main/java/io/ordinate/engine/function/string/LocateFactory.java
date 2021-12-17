package io.ordinate.engine.function.string;

import io.mycat.calcite.sqlfunction.stringfunction.LeftFunction;
import io.mycat.calcite.sqlfunction.stringfunction.LocateFunction;
import io.mycat.calcite.sqlfunction.stringfunction.UnhexFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.*;
import io.ordinate.engine.function.constant.IntConstant;
import io.ordinate.engine.record.Record;

import java.util.Collections;
import java.util.List;

public class LocateFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "locate()";
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
        public boolean isNull(Record rec) {
            return isNull;
        }

        @Override
        public int getInt(Record rec) {
            Function one = this.args.get(0);
            Function two = this.args.get(1);
            Function three;
            if (this.args.size() > 2) {
                three = this.args.get(2);
            } else {
                three = IntConstant.newInstance(0);
            }
            isNull = one.isNull(rec) || two.isNull(rec) || three.isNull(rec);
            if (isNull) return 0;
            Integer locate = LocateFunction.locate(one.getString(rec).toString(), two.getString(rec).toString(), three.getInt(rec));
            isNull = locate == null;
            if (isNull) return 0;
            return locate;
        }
    }
}

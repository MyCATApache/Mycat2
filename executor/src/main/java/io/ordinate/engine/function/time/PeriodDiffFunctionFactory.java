package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.PeriodAddFunction;
import io.mycat.calcite.sqlfunction.datefunction.PeriodDiffFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.record.Record;

import java.util.List;

public class PeriodDiffFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "PERIOD_DIFF(int,int):int";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }


    private static final class Func extends IntFunction {

        List<Function> args;
        boolean isNull;

        public Func(List<Function> args) {
            this.args = args;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }

        @Override
        public int getInt(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);

            int a = one.getInt(rec);
            int b = two.getInt(rec);
            isNull = one.isNull(rec) || two.isNull(rec);
            if (isNull) {
                return 0;
            }
            return (int) PeriodDiffFunction.periodDiff(a, b);
        }
    }
}

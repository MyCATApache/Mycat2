package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.TimeDiff2Function;
import io.mycat.calcite.sqlfunction.datefunction.TimeFormatFunction;
import io.mycat.calcite.sqlfunction.datefunction.Timestamp2Function;
import io.mycat.calcite.sqlfunction.datefunction.TimestampFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.record.Record;

import java.time.Duration;
import java.util.List;

public class TimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "timestamp(date,time):datetime";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }

    private static final class Func extends io.ordinate.engine.function.DatetimeFunction {

        final List<Function> args;
        boolean isNull;

        public Func(List<Function> args) {
            this.args = args;
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }

        @Override
        public long getDatetime(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);
            long date = one.getDate(rec);
            long time = two.getTime(rec);
            isNull = one.isNull(rec) || two.isNull(rec);
            if (isNull) return 0;
            return date + time;
        }
    }
}

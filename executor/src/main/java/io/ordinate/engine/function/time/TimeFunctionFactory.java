package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.TimeFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.DatetimeFunction;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.record.Record;

import java.sql.Time;
import java.time.Duration;
import java.time.LocalTime;
import java.util.List;

public class TimeFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "time(string):time";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }

    private static final class Func extends io.ordinate.engine.function.TimeFunction {

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
        public long getTime(Record rec) {
            Function function = args.get(0);
            CharSequence string = function.getString(rec);
            isNull = function.isNull(rec);
            if (isNull) return 0;
            Duration time = TimeFunction.time(string.toString());
            return time.toMillis();
        }
    }
}

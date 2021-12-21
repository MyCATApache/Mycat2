package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.TimeFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.record.Record;

import java.time.Duration;
import java.util.List;

public class TimeToSecFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "time_to_sec(time):long";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }

    private static final class Func extends io.ordinate.engine.function.LongFunction {

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
        public long getLong(Record rec) {
            Function function = args.get(0);
            long time = function.getTime(rec);
            isNull = function.isNull(rec);
            if (isNull) return 0;
            return Duration.ofMillis(time).getSeconds();
        }
    }
}

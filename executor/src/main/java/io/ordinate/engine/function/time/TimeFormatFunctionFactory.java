package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.TimeFormatFunction;
import io.mycat.calcite.sqlfunction.datefunction.TimeFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.record.Record;

import java.time.Duration;
import java.util.List;

public class TimeFormatFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "time_format(time,string):string";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }

    private static final class Func extends io.ordinate.engine.function.StringFunction {

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
        public String getString(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);
            long time = one.getTime(rec);
            CharSequence format = two.getString(rec);
            isNull = one.isNull(rec) || two.isNull(rec);
            if (isNull) return null;
            return TimeFormatFunction.timeFormat(Duration.ofMillis(time), format.toString());
        }
    }
}

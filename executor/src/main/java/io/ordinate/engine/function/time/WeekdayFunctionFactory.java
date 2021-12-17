package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.WeekDayFunction;
import io.mycat.calcite.sqlfunction.datefunction.WeekFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.record.Record;

import java.sql.Date;
import java.util.List;

public class WeekdayFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "weekday(date):long";
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
            long value = function.getDate(rec);
            isNull = function.isNull(rec);
            if (isNull) return 0;
            return WeekDayFunction.weekDay(new Date(value).toLocalDate());
        }
    }
}

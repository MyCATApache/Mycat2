package io.ordinate.engine.function.time;

import io.mycat.util.DateUtil;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.record.Record;

import java.sql.Date;
import java.util.List;

public class DayFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "day(date):int";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new DayFunction(args);
    }
    class DayFunction extends IntFunction {
        List<Function> args;
        boolean isNull;

        public DayFunction(List<Function> args) {
            this.args = args;
        }

        @Override
        public List<Function> getArgs() {
            return args;
        }

        @Override
        public int getInt(Record rec) {
            Function one = args.get(0);

            long date1 = one.getDate(rec);

            isNull = one.isNull(rec);
            if (isNull)return 0;

            return DateUtil.getDay(new Date(date1));
        }

    };
}

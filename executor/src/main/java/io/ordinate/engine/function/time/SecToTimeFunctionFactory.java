package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.SecToTimeFunction;
import io.mycat.calcite.sqlfunction.datefunction.SecondFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.TimeFunction;
import io.ordinate.engine.record.Record;

import java.sql.Time;
import java.time.Duration;
import java.time.LocalTime;
import java.util.List;

public class SecToTimeFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "sec_to_time(long):time";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }


    private static final class Func extends TimeFunction {

        List<Function> args ;
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
            Function one = args.get(0);

            long a = one.getLong(rec);
            isNull = one.isNull(rec);
            if (isNull){
                return 0;
            }
            return Duration.ofSeconds(a).toMillis();
        }
    }
}

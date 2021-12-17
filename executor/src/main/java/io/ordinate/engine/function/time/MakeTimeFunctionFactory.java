package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.MakeDateFunction;
import io.mycat.calcite.sqlfunction.datefunction.MakeTimeFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.DateFunction;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.TimeFunction;
import io.ordinate.engine.record.Record;

import java.sql.Date;
import java.sql.Time;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

public class MakeTimeFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "maketime(int,int,int):time";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }


    public static void main(String[] args) {
        int i = "magd0b".hashCode() % 16;
        int i1 = "maGD0b".hashCode() % 16;
        System.out.println();
    }

    private static final class Func extends TimeFunction {

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
        public long getTime(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);
            Function three = args.get(2);

            int oneArg = one.getInt(rec);
            int twoArg = two.getInt(rec);
            int threeArg = three.getInt(rec);

            isNull = one.isNull(rec) || two.isNull(rec) || three.isNull(rec);

            if (isNull) {
                return 0;
            }
            return Duration.ofHours(oneArg).plusMinutes(twoArg).plusSeconds(threeArg).toMillis();
        }
    }
}

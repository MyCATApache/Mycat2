package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.QuarterFunction;
import io.mycat.calcite.sqlfunction.datefunction.SecondFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.record.Record;

import java.sql.Date;
import java.time.Duration;
import java.util.List;

public class SecondFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "second(time):int";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }


    private static final class Func extends IntFunction {

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
        public int getInt(Record rec) {
            Function one = args.get(0);

            long a = one.getTime(rec);
            isNull = one.isNull(rec);
            if (isNull){
                return 0;
            }
            return SecondFunction.second(Duration.ofMillis(a));
        }
    }
}

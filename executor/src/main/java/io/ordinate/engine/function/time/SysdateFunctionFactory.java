package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.SysDateFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.DatetimeFunction;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.TimeFunction;
import io.ordinate.engine.record.Record;

import java.time.LocalTime;
import java.util.List;

public class SysdateFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "sysdate():datetime";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func();
    }

    private static final class Func extends DatetimeFunction {



        public Func() {

        }

        @Override
        public boolean isNull(Record rec) {
            return false;
        }

        @Override
        public long getDatetime(Record rec) {
            return System.currentTimeMillis();
        }
    }
}

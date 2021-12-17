package io.ordinate.engine.function.time;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.DateFunction;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.TimeFunction;
import io.ordinate.engine.record.Record;

import java.time.LocalTime;
import java.util.List;

public class CurTimeFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "CURTIME():time";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func();
    }

    private static final class Func extends TimeFunction {



        public Func() {

        }

        @Override
        public boolean isNull(Record rec) {
            return false;
        }

        @Override
        public long getTime(Record rec) {
            return LocalTime.now().toNanoOfDay()/1000;
        }
    }
}

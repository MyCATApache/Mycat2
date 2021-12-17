package io.ordinate.engine.function.time;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.DateFunction;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.record.Record;

import java.util.List;

public class CurrentDateFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "CURRENT_DATE():date";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func();
    }

    private static final class Func extends DateFunction {



        public Func() {

        }

        @Override
        public boolean isNull(Record rec) {
            return false;
        }

        @Override
        public long getDate(Record rec) {
            return System.currentTimeMillis();
        }
    }
}

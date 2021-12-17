package io.ordinate.engine.function.time;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.*;
import io.ordinate.engine.record.Record;
import io.questdb.std.datetime.microtime.Timestamps;

import java.sql.Date;
import java.util.List;

public class CurDateFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "curdate():date";
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

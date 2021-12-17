package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.DaynameFunction;
import io.mycat.util.DateUtil;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.sql.Date;
import java.util.List;

public class DayNameFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "dayname(date):string";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new DayNameFunction(args);
    }
    class DayNameFunction extends StringFunction {
        List<Function> args;
        boolean isNull;

        public DayNameFunction(List<Function> args) {
            this.args = args;
        }

        @Override
        public List<Function> getArgs() {
            return args;
        }

        @Override
        public String getString(Record rec) {
            Function one = args.get(0);

            long date1 = one.getDate(rec);

            isNull = one.isNull(rec);
            if (isNull)return null;

            return DaynameFunction.dayname(new Date(date1).toLocalDate());
        }

    };
}

package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.DateFormatFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.function.TimeFunction;
import io.ordinate.engine.record.Record;

import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

public class DateFormatFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "DATE_FORMAT(string,string):string";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }


    private static final class Func extends StringFunction {

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
        public CharSequence getString(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);

            String oneArg = Optional.ofNullable(one.getString(rec)).map(i->i.toString()).orElse(null);
            String twoArg = Optional.ofNullable(two.getString(rec)).map(i->i.toString()).orElse(null);

            isNull = one.isNull(rec)||two.isNull(rec);

            if (isNull){
                return null;
            }
            return DateFormatFunction.dateFormat(oneArg,twoArg);
        }
    }
}

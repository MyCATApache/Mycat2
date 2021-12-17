package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.StringToTimestampFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.*;
import io.ordinate.engine.function.cast.CastStringToDateFunctionFactory;
import io.ordinate.engine.record.Record;

import java.sql.Time;
import java.time.LocalTime;
import java.util.List;

public class StrToDateFunctionFactory extends CastStringToDateFunctionFactory {

    @Override
    public String getSignature() {
        return "str_to_date(string,string):date";
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
        public String getString(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);

            CharSequence a = one.getString(rec);
            CharSequence b = two.getString(rec);
            isNull = one.isNull(rec)||two.isNull(rec);
            if (isNull){
                return null;
            }
            return io.mycat.calcite.sqlfunction.datefunction.StrToDateFunction.strToDate(a.toString(),b.toString());
        }
    }
}

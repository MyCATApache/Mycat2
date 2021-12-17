package io.ordinate.engine.function.time;

import io.mycat.calcite.sqlfunction.datefunction.DateFormatFunction;
import io.mycat.calcite.sqlfunction.datefunction.MakeDateFunction;
import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.DateFunction;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.StringFunction;
import io.ordinate.engine.record.Record;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

public class MakeDateFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "makedate(int,int):date";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Func(args);
    }


    private static final class Func extends DateFunction {

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
        public long getDate(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);


            int oneArg =  one.getInt(rec);
            int twoArg = two.getInt(rec);

            isNull = one.isNull(rec)||two.isNull(rec);

            if (isNull){
                return 0;
            }
            LocalDate localDate = MakeDateFunction.makeDate(oneArg, twoArg);
            return Date.valueOf(localDate).getTime();
        }
    }
}

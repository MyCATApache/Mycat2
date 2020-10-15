package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.Period;
import java.time.YearMonth;

public class LastDayFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(LastDayFunction.class,
            "lastDay");
    public static LastDayFunction INSTANCE = new LastDayFunction();

    public LastDayFunction() {
        super("LAST_DAY", scalarFunction);
    }
    public static LocalDate lastDay(LocalDate date) {
        if (date == null)return null;
        return    YearMonth.from(date).atEndOfMonth();
    }
    public static LocalDate lastDay(Period date){
        if (date == null){
            return null;
        }
        if (date.isNegative()){
            throw new UnsupportedOperationException();
        }
       return LocalDate.of(date.getMonths(),date.getMonths(),date.getDays());
    }

}


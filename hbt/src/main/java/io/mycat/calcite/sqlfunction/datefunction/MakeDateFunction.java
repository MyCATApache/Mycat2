package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class MakeDateFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(MakeDateFunction.class,
            "makeDate");
    public static MakeDateFunction INSTANCE = new MakeDateFunction();

    public MakeDateFunction() {
        super("MAKEDATE",
                scalarFunction
        );
    }

    public static LocalDate makeDate(Integer year,Integer dayofyear) {
        if (dayofyear == null||!(dayofyear>0)){
            return null;
        }
        return LocalDate.ofYearDay(year,dayofyear);
    }
}

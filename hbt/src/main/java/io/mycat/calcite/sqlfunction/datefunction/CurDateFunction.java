package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class CurDateFunction  extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(CurDateFunction.class,
            "curDate");
    public static CurDateFunction INSTANCE = new CurDateFunction();

    public CurDateFunction() {
        super("CURDATE", scalarFunction);
    }

    public static LocalDate curDate(){
        return LocalDate.now();
    }
}

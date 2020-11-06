package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

public class NowFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(NowFunction.class,
            "now");
    public static NowFunction INSTANCE = new NowFunction();

    public NowFunction() {
        super("NOW",
                scalarFunction
        );
    }

    public static LocalDateTime now(@Parameter(name = "precision", optional = true) Integer precision) {
        if (precision == null) {
            return LocalDateTime.now(ZoneId.systemDefault()).now().withNano(0);
        }
        LocalDateTime now = LocalDateTime.now();
        int nano = now.getNano();
        //999,999,999

        int i1 =(int) Math.pow(10,(9 - precision));
        nano= nano/i1*i1;
        return now.withNano(nano);
    }
}

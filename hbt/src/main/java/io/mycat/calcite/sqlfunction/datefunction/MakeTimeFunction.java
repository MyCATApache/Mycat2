package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.Duration;
import java.time.LocalDate;

public class MakeTimeFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(MakeTimeFunction.class,
            "makeTime");
    public static MakeTimeFunction INSTANCE = new MakeTimeFunction();

    public MakeTimeFunction() {
        super("MAKETIME",
                scalarFunction
        );
    }

    public static Duration makeTime(Integer hour,Integer minute,Integer second) {
       if (hour == null||minute == null||second == null){
           return null;
       }
       if (minute>60||minute<0){
           return null;
       }
        if (second>60||second<0){
            return null;
        }
        if (hour>838 ){
            hour = 838;
        }
        if (hour<-838 ){
            hour = -838;
        }
        if (hour>=0) {
            return Duration.ofHours(hour).plusMinutes(minute).plusSeconds(second);
        }
        return Duration.ofHours(-hour).plusMinutes(minute).plusSeconds(second).negated();
    }
}

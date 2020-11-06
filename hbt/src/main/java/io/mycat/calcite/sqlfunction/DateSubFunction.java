package io.mycat.calcite.sqlfunction;

import java.sql.Time;
import java.util.Date;

public class DateSubFunction {
    public static Date eval(Date arg0, Time arg1) {
        if (arg0 == null||arg1==null){
            return null;
        }
        return new Date(arg0.getTime()-arg1.getTime());
    }
}
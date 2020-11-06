package io.mycat.calcite.sqlfunction.datefunction;

import com.github.sisyphsu.dateparser.DateParserUtils;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SubTimeFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(SubTimeFunction.class,
            "subTime");
    public static SubTimeFunction INSTANCE = new SubTimeFunction();

    public SubTimeFunction() {
        super("SUBTIME",
                scalarFunction
        );
    }

    public static String subTime(String time1, String time2) {
     return   AddTimeFunction.addTime(time1, time2,true);
    }

}

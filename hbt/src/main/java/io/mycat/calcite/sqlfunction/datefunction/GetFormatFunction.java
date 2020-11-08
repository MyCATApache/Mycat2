package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.time.LocalDateTime;
import java.util.Locale;

public class GetFormatFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(GetFormatFunction.class,
            "getFormat");
    public static GetFormatFunction INSTANCE = new GetFormatFunction();

    public GetFormatFunction() {
        super("GET_FORMAT", scalarFunction);
    }

    public static String getFormat(String date, String format) {
        String agr0 = date.toUpperCase();
        String agr1 = format.toUpperCase();
        switch (agr0) {
            case "DATE":
                switch (agr1) {
                    case "USA":
                        return "%m.%d.%Y";
                    case "JIS":
                    case "ISO":
                        return "%Y-%m-%d";
                    case "EUR":
                        return "%d.%m.%Y";
                    case "INTERNAL":
                        return "%Y%m%d";
                }
                break;
            case "DATETIME":
                switch (agr1) {
                    case "USA":
                    case "EUR":
                        return "%Y-%m-%d %H.%i.%s";
                    case "JIS":
                    case "ISO":
                        return "%Y-%m-%d %H:%i:%s";
                    case "INTERNAL":
                        return "%Y%m%d%H%i%s";
                }
                break;
            case "TIME":
                switch (agr1) {
                    case "USA":
                        return "%h:%i:%s %p";
                    case "JIS":
                    case "ISO":
                        return "%H:%i:%s";
                    case "EUR":
                        return "%H.%i.%s";
                    case "INTERNAL":
                        return "%H%i%s";
                }
                break;
        }
        return null;
    }
}


package io.mycat.calcite.sqlfunction.datefunction;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

public class StringToTimestampFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(StringToTimestampFunction.class,
            "stringToTimestamp");
    public static StringToTimestampFunction INSTANCE = new StringToTimestampFunction();


    public StringToTimestampFunction() {
        super(new SqlIdentifier("stringToTimestamp", SqlParserPos.ZERO),
                ReturnTypes.explicit(scalarFunction.getReturnType(MycatCalciteSupport.INSTANCE.TypeFactory)),
                InferTypes.explicit(getRelDataType(scalarFunction)),
            null,
                ImmutableList.of(), scalarFunction);
    }
    public static Date stringToTimestamp(Object text){
        return null;
    }
    public static Date stringToTimestamp(Date text) {
       return null;
    }

    //SqlParserUtil
    //DateTimeUtils
    //SqlLiteral
    public static String stringToTime(String text) {
        if (text == null) {
            return null;
        }
        text = text.trim();
        if (text.isEmpty()) {
            return null;
        }
        int days = 0;
        int h = 0;
        int m = 0;
        int second = 0;
        int optional = text.lastIndexOf('.');
        int second_part = 0;
        if (optional != -1) {
            second_part = Integer.parseInt(text.substring(optional + 1));
            text = text.substring(0, optional);
        }
        switch (text.charAt(0)) {
            case '-': {//[-] DAYS [H]H:MM:SS
                int suff = text.indexOf(" ");
                days = Integer.parseInt(text.substring(0, suff));
                String[] split = text.split(":");
                h = Integer.parseInt(split[0]);
                m = Integer.parseInt(split[1]);
                second = Integer.parseInt(split[2]);
                break;
            }
            default: {
                String[] split = text.split(":");
                split[0] = split[0].trim();
                if (split.length == 3) {
                    if (split[0].contains(" ")) {
                        //DAYS [H]H:MM:SS
                        String[] s = split[0].split(" ");
                        days = Integer.parseInt(s[0]);
                        h = Integer.parseInt(s[0]);
                        m = Integer.parseInt(split[1]);
                        second = Integer.parseInt(split[2]);
                        break;
                    } else {
                        //[H]H:MM:SS
                        h = Integer.parseInt(split[0]);
                        m = Integer.parseInt(split[1]);
                        second = Integer.parseInt(split[2]);
                        break;
                    }
                } else if (split.length == 2) {
                    m = Integer.parseInt(split[0]);
                    second = Integer.parseInt(split[1]);
                    break;
                } else if (split.length == 1) {
                    String shor = split[0];
                    switch (shor.length()) {
                        case 1: {
                            //S
                            second = Integer.parseInt(shor);
                            break;
                        }
                        case 2: {
                            //[S]S
                            second = Integer.parseInt(shor);
                            break;
                        }
                        case 3: {
                            //MSS
                            m = Integer.parseInt(shor.substring(0, 1));
                            second = Integer.parseInt(shor.substring(1, 2));
                            break;
                        }
                        case 4: {
                            //[M]MSS
                            m = Integer.parseInt(shor.substring(0, 2));
                            second = Integer.parseInt(shor.substring(2, 4));
                            break;
                        }
                        case 5: {
                            //HMMSS
                            h = Integer.parseInt(shor.substring(0, 1));
                            m = Integer.parseInt(shor.substring(1, 3));
                            second = Integer.parseInt(shor.substring(3, 5));
                            break;
                        }
                        case 6: {
                            //[H]HMMSS
                            h = Integer.parseInt(shor.substring(0, 2));
                            m = Integer.parseInt(shor.substring(2, 4));
                            second = Integer.parseInt(shor.substring(4, 6));
                            break;
                        }
                    }

                    break;
                }
            }
        }
        long l = LocalTime.of(h, m, second, second_part).toNanoOfDay();
//        if (days > 0) {
//            return Duration.ofDays(days).plusNanos(l).toMillis();
//        } else {
//            return Duration.ofNanos(l).toMillis();
//        }

        return null;

    }
//        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("YYYY-MM-DD HH:mm:ss.n");
//
//
//        final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
//                .parseCaseInsensitive()
//                .optionalStart()
//                .append(new DateTimeFormatterBuilder()
//                        .optionalStart()
//                        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
//                        .optionalEnd()
//                        .optionalStart()
//                        .appendLiteral('-')
//                        .optionalEnd()
//                        .optionalStart()
//                        .appendValue(MONTH_OF_YEAR, 2)
//                        .optionalEnd()
//                        .optionalStart()
//                        .appendLiteral('-')
//                        .appendValue(DAY_OF_MONTH, 2)
//                        .optionalEnd().toFormatter())
//                .optionalEnd()
//                .optionalStart()
//                .appendLiteral(' ')
//                .optionalEnd()
//                .optionalStart()
//                .append(ISO_LOCAL_TIME)
//                .optionalEnd()
//                .toFormatter();
//
//        final TemporalAccessor temporalAccessor = formatter.parse(text);
//         LocalDateTime localDateTime = LocalDateTime.of(
//                secureGet(temporalAccessor, ChronoField.YEAR),
//                secureGet(temporalAccessor, ChronoField.MONTH_OF_YEAR),
//                secureGet(temporalAccessor, ChronoField.DAY_OF_MONTH),
//                secureGet(temporalAccessor, ChronoField.HOUR_OF_DAY),
//                secureGet(temporalAccessor, ChronoField.MINUTE_OF_HOUR),
//                secureGet(temporalAccessor, ChronoField.SECOND_OF_MINUTE),
//                secureGet(temporalAccessor, ChronoField.NANO_OF_SECOND)
//        );
//
////
////        TemporalAccessor parse = formatter.parse(text);
//        return    Timestamp.valueOf(localDateTime).getTime();


    /**
     * 安全获取时间的某个属性
     *
     * @param temporalAccessor 需要获取的时间对象
     * @param chronoField      需要获取的属性
     * @return 时间的值，如果无法获取则默认为 0
     */
    private static int secureGet(TemporalAccessor temporalAccessor, ChronoField chronoField) {
        if (temporalAccessor.isSupported(chronoField)) {
            return temporalAccessor.get(chronoField);
        }
        return 0;
    }
}
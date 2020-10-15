package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TimeFormatFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TimeFormatFunction.class,
            "timeFormat");
    public static TimeFormatFunction INSTANCE = new TimeFormatFunction();

    public TimeFormatFunction() {
        super("TIME_FORMAT",
                scalarFunction
        );
    }

    public static String timeFormat(Duration duration,String format) {
        if (duration==null||format == null) {
            return null;
        }
        long seconds = duration.getSeconds();
        int SECONDS_PER_HOUR = 60*60;
        int SECONDS_PER_MINUTE = 60;
        int hours = (int)(seconds / SECONDS_PER_HOUR);
        int minutes = (int) ((seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
        int secs = (int) (seconds % SECONDS_PER_MINUTE);
       return timeFormat(format,hours,minutes,secs);
    }
    @Nullable
    private static String timeFormat(String format, int hour, int minute, int seconds) {
        StringBuilder dateTimeFormatterBuilder = new StringBuilder();
        int length = format.length();
        for (int i = 0; i < length; i++) {
            int next = i + 1;
            if (format.charAt(i) == '%' && next != length) {
                char c = format.charAt(next);
                if (isOption(c)) {
                    i+=1;
                    switch (c) {
                        case 'f': {
                            dateTimeFormatterBuilder.append(String.format("%06d",seconds));
                            break;
                        }
                        case 'H': {
                            dateTimeFormatterBuilder.append(String.format("%02d",hour));
                            break;
                        }
                        case 'h':
                        case 'I': {
                            dateTimeFormatterBuilder.append(String.format("%02d",hour%24));
                            break;
                        }
                        case 'i': {
                            dateTimeFormatterBuilder.append(String.format("%02d",minute));
                            break;
                        }
                        case 'k': {
                            dateTimeFormatterBuilder.append(String.format("%01d",hour));
                            break;
                        }
                        case 'l': {
                            dateTimeFormatterBuilder.append(String.format("%01d",hour%24));
                            break;
                        }
                        case 'p': {
                            dateTimeFormatterBuilder.append(String.format("%01d",(hour%12)>0?"PM":"AM"));
                            break;
                        }
                        case 'S':
                        case 's': {
                            dateTimeFormatterBuilder.append(String.format("%02d",seconds));
                            break;
                        }
                        case '#': {
                            for (int j = next; j < length; j++) {
                                if (Character.isDigit(format.charAt(j))) {
                                    continue;
                                } else {
                                    i = j;
                                }
                            }
                            break;
                        }
                        case '.': {
                            for (int j = next; j < length; j++) {
                                if (isPunctuation(format.charAt(j))) {
                                    continue;
                                } else {
                                    i = j;
                                }
                            }
                            break;
                        }
                        case '@': {
                            for (int j = next; j < length; j++) {
                                if (Character.isAlphabetic(format.charAt(j))) {
                                    continue;
                                } else {
                                    i = j;
                                }
                            }
                            break;
                        }
                        case '%': {
                            dateTimeFormatterBuilder
                                    .append('%');
                            break;
                        }
                        default: {
                            throw new UnsupportedOperationException();
                        }

                    }
                } else {
                    dateTimeFormatterBuilder.append(c);
                }
            } else {
                dateTimeFormatterBuilder.append(format.charAt(i));
            }
        }
        return dateTimeFormatterBuilder.toString();
    }

    public static boolean isOption(char c) {
        switch (c) {
            case 'a':
            case 'b':
            case 'c':
            case 'D':
            case 'd':
            case 'e':
            case 'f':
            case 'H':
            case 'h':
            case 'I':
            case 'i':
            case 'j':
            case 'k':
            case 'l':
            case 'M':
            case 'm':
            case 'p':
            case 'r':
            case 'S':
            case 's':
            case 'T':
            case 'U':
            case 'u':
            case 'V':
            case 'v':
            case 'W':
            case 'w':
            case 'X':
            case 'x':
            case 'Y':
            case 'y':
            case '#':
            case '.':
            case '@':
            case '%':
                return true;
            default: {
                return false;
            }
        }
    }

    static boolean isPunctuation(char ch) {
        if (isCjkPunc(ch)) return true;
        if (isEnPunc(ch)) return true;

        if (0x2018 <= ch && ch <= 0x201F) return true;
        if (ch == 0xFF01 || ch == 0xFF02) return true;
        if (ch == 0xFF07 || ch == 0xFF0C) return true;
        if (ch == 0xFF1A || ch == 0xFF1B) return true;
        if (ch == 0xFF1F || ch == 0xFF61) return true;
        if (ch == 0xFF0E) return true;
        if (ch == 0xFF65) return true;

        return false;
    }

    static boolean isEnPunc(char ch) {
        if (0x21 <= ch && ch <= 0x22) return true;
        if (ch == 0x27 || ch == 0x2C) return true;
        if (ch == 0x2E || ch == 0x3A) return true;
        if (ch == 0x3B || ch == 0x3F) return true;

        return false;
    }

    static boolean isCjkPunc(char ch) {
        if (0x3001 <= ch && ch <= 0x3003) return true;
        if (0x301D <= ch && ch <= 0x301F) return true;

        return false;
    }
}

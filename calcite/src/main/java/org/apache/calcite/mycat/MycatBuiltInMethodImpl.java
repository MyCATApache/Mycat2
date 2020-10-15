package org.apache.calcite.mycat;

import com.github.sisyphsu.dateparser.DateParserUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.util.DateTimeStringUtils;
import org.apache.calcite.util.TimestampString;
import org.apache.commons.lang3.time.DateParser;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.WeekFields;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class MycatBuiltInMethodImpl {
    public static double timestampToDouble(LocalDateTime localDateTime) {
        return Double.parseDouble(localDateTime.format(new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR,4)
                .appendValue(ChronoField.MONTH_OF_YEAR,2)
                .appendValue(ChronoField.DAY_OF_MONTH,2)
                .appendValue(ChronoField.HOUR_OF_DAY,2)
                .appendValue(ChronoField.MINUTE_OF_HOUR,2)
                .appendValue(ChronoField.SECOND_OF_MINUTE,2)
                .appendLiteral('.')
                .appendValue(ChronoField.MICRO_OF_SECOND)
                .toFormatter()));
    }
    public static long extract(TimeUnitRange unit, String dateTimeText) {
        Object o = MycatBuiltInMethodImpl.parseUnknownDateTimeText(dateTimeText);
        if(o instanceof Temporal) {
            Temporal temporal = (Temporal)o;
            return extract(unit, temporal);
        }else if (o instanceof Duration){
            Duration duration = (Duration)o;
            int days = (int) duration.toDays();
            if (days == 0){
                LocalTime plus = LocalTime.ofNanoOfDay(0).plus(duration);
                return extract(unit, plus);
            }
            LocalDateTime plus = LocalDateTime.of(1,1,days,0,0)
                    .plus(duration.minusDays(1));
            return extract(unit, plus);
        }
        throw new UnsupportedOperationException();
    }
    public static long myPackedTimeGetIntPart(long x) {
        return x >> 24;
    }

    public static long myPackedTimeGetFracPart(long x) {
        return ((x) % (1L << 24));
    }
    public static Long dateToLong(LocalDate tmp){
        if (tmp == null){
            return null;
        }
        long year = tmp.getYear();
        int month = tmp.getMonth().getValue();
        int dayOfMonth = tmp.getDayOfMonth();
        return Long.parseLong( String.format("%04d%02d%02d",year,month,dayOfMonth));
    }
    public static LocalDate longToUnixDate(Long tmp){
        long time = DateParserUtils.parseDate(Long.toString(tmp)).getTime();
        return  LocalDate.ofEpochDay(time);
    }
    public static LocalDateTime longToUnixTimestamp(Long tmp){
        LocalDateTime localDateTime = DateParserUtils.parseDateTime(Long.toString(tmp));
        return localDateTime;
    }
    private static long extract(TimeUnitRange unit, Temporal temporal) {
        switch (unit) {
            case YEAR:
                return temporal.get(ChronoField.YEAR);
            case YEAR_TO_MONTH:
                return temporal.get(ChronoField.YEAR) * 100L + temporal.get(ChronoField.MONTH_OF_YEAR);
            case MONTH:
                return temporal.get(ChronoField.MONTH_OF_YEAR);
            case DAY:
                return temporal.get(ChronoField.DAY_OF_MONTH);
            case DAY_TO_HOUR:
                return temporal.get(ChronoField.DAY_OF_MONTH) * 100L + temporal.get(ChronoField.HOUR_OF_DAY);
            case DAY_TO_MINUTE:
                return temporal.get(ChronoField.DAY_OF_MONTH) * 10000L
                        + temporal.get(ChronoField.HOUR_OF_DAY) * 100L + temporal.get(ChronoField.MINUTE_OF_HOUR);
            case DAY_TO_SECOND:
                return temporal.get(ChronoField.DAY_OF_MONTH) * 1000000L
                        + temporal.get(ChronoField.HOUR_OF_DAY) * 10000L + temporal.get(ChronoField.MINUTE_OF_HOUR) * 100
                        + temporal.get(ChronoField.SECOND_OF_MINUTE);

            case HOUR:
                return temporal.get(ChronoField.HOUR_OF_DAY);
            case HOUR_TO_MINUTE:
                return temporal.get(ChronoField.HOUR_OF_DAY) * 100 + temporal.get(ChronoField.MINUTE_OF_HOUR);
            case HOUR_TO_SECOND:
                return temporal.get(ChronoField.HOUR_OF_DAY) * 10000 + temporal.get(ChronoField.MINUTE_OF_HOUR) * 100
                        + temporal.get(ChronoField.SECOND_OF_MINUTE);
            case MINUTE:
                return temporal.get(ChronoField.MINUTE_OF_HOUR);
            case MINUTE_TO_SECOND:
                return temporal.get(ChronoField.MINUTE_OF_HOUR) * 100 + temporal.get(ChronoField.SECOND_OF_MINUTE);
            case SECOND:
                return temporal.get(ChronoField.SECOND_OF_MINUTE);
            case ISOYEAR:
                return temporal.get(ChronoField.YEAR);
            case QUARTER:
                return temporal.get(IsoFields.QUARTER_OF_YEAR);
            case WEEK:
                return temporal.get(WeekFields.ISO.weekOfYear());
            case MILLISECOND:
                return temporal.get(ChronoField.MILLI_OF_SECOND);
            case MICROSECOND:
                return temporal.get(ChronoField.MICRO_OF_SECOND);
            case NANOSECOND:
                return temporal.get(ChronoField.NANO_OF_SECOND);
            case DOW:
                return temporal.get(ChronoField.DAY_OF_WEEK);
            case ISODOW:
                throw new UnsupportedOperationException();
            case DOY:
                throw new UnsupportedOperationException();
            case EPOCH:
                return temporal.get(ChronoField.EPOCH_DAY);
            case DECADE:
                throw new UnsupportedOperationException();
            case CENTURY:
                throw new UnsupportedOperationException();
            case MILLENNIUM:
                return temporal.get(ChronoField.MILLI_OF_SECOND);
            case SECOND_TO_MICROSECOND:
                return temporal.get(ChronoField.SECOND_OF_DAY) * 1000000L + temporal.get(ChronoField.NANO_OF_SECOND);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static String dateAddString(String s, Duration duration) {
        if (s == null || duration == null) {
            return null;
        }
        Temporal temporal = timestampStringToUnixTimestamp(s);
        LocalDateTime of;
        boolean date = false;
        if (temporal instanceof LocalDate) {
            date = true;
            of = LocalDateTime.of((LocalDate) temporal, LocalTime.ofSecondOfDay(0)).plus(duration);
        } else {
            of = ((LocalDateTime) temporal).plus(duration);
        }
        if (of.toLocalTime().equals(LocalTime.ofSecondOfDay(0))) {
            return of.toLocalDate().toString();
        } else {
            return of.toLocalDate() + " " + of.toLocalTime();
        }
    }

    public static String dateSubString(String s, Duration duration) {
        if (s == null || duration == null) {
            return null;
        }
        Temporal temporal = timestampStringToUnixTimestamp(s);
        LocalDateTime of;
        boolean date = false;
        if (temporal instanceof LocalDate) {
            date = true;
            of = LocalDateTime.of((LocalDate) temporal, LocalTime.ofSecondOfDay(0)).minus(duration);
        } else {
            of = ((LocalDateTime) temporal).minus(duration);
        }
        if (of.toLocalTime().equals(LocalTime.ofSecondOfDay(0))) {
            return of.toLocalDate().toString();
        } else {
            return of.toLocalDate() + " " + of.toLocalTime();
        }
    }
    //unsupported SELECT TIMEDIFF('2000:01:01 00:00:00', '2000:01:01 00:00:00.000001');
    public static Object parseUnknownDateTimeText(String s) {
        if (s.contains(":")&&!s.contains("-")){
            String[] split = s.split(":");
          return   Duration.ofHours(Long.parseLong(split[split.length-3]))
                    .plusMinutes(Long.parseLong(split[split.length-2]))
                    .plusSeconds(Long.parseLong(split[split.length-1]));
        }
        return timestampStringToUnixTimestamp(s);
    }
    public static Temporal timestampStringToUnixTimestamp(String s) {
        if (s == null) {
            return null;
        }
        int i = s.lastIndexOf(".");
        if (i == -1) {
            i = s.lastIndexOf(" ");
            if (i != -1) {
                return new Timestamp(new TimestampString(s).getMillisSinceEpoch()).toLocalDateTime();
            }
            if (s.contains("-")){
                //SELECT DATE_FORMAT('2006-06-00', '%d'); unsupport
                return LocalDate.parse(s).atStartOfDay();
            }
            if (s.contains(":")){
                return LocalTime.parse(s);
            }
            throw new UnsupportedOperationException();
        } else {
            if (s.contains(" ")) {
                String[] uni = s.split(" ");
                return LocalDateTime.of(LocalDate.parse(uni[0]), LocalTime.parse(uni[1]));
            }
            return LocalTime.parse(s);
        }
    }

    public static Temporal timestampStringToUnixDate(String s) {
        Temporal temporal = timestampStringToUnixTimestamp(s);
        if (temporal instanceof LocalDate) {
            return temporal;
        }
        if (temporal instanceof LocalDateTime) {
            return ((LocalDateTime) temporal).toLocalDate();
        }
        throw new UnsupportedOperationException();
    }

    public static void main(String[] args) {
        Timestamp timestamp = new Timestamp(0, 0, 0, 0, 0, 0, 0);
        int year = timestamp.getYear();
    }

    public static Duration timeStringToUnixDate(String s) {
        return timeStringToTimeDuration(s);
    }

    public static Duration timeStringToTimeDuration(String text) {
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
            String substring = text.substring(optional + 1);
            second_part = Integer.parseInt(
                    substring
            )*(int)Math.pow(10,9-substring.length());
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
                if (text.contains(" ")){
                    text = text.split(" ")[1];
                }
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
        return Duration.ofDays(days).plusHours(h).plusMinutes(m).plusSeconds(second).plusNanos(second_part);

    }

    /**
     * SELECT LAST_DAY('2003-03-32'); unsupported
     * @param s
     * @return
     */
    public static LocalDate dateStringToUnixDate(String s) {
        try {
            if (s.contains(" ")) {
                s = s.split(" ")[0];
            }
            int hyphen1 = s.indexOf(45);
            int y;
            int m;
            int d;
            if (hyphen1 < 0) {
                y = Integer.parseInt(s.trim());
                m = 1;
                d = 1;
            } else {
                y = Integer.parseInt(s.substring(0, hyphen1).trim());
                int hyphen2 = s.indexOf(45, hyphen1 + 1);
                if (hyphen2 < 0) {
                    m = Integer.parseInt(s.substring(hyphen1 + 1).trim());
                    d = 1;
                } else {
                    m = Integer.parseInt(s.substring(hyphen1 + 1, hyphen2).trim());
                    d = Integer.parseInt(s.substring(hyphen2 + 1).trim());
                }
            }

            return LocalDate.of(y, m, d);
        }catch (Throwable e){
            return null;
        }
    }

}

package org.apache.calcite.mycat;

import com.github.sisyphsu.dateparser.DateParserUtils;
import io.mycat.MycatTimeUtil;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.WeekFields;

public class MycatBuiltInMethodImpl {
    public static String dateSubString(java.lang.String date,  java.time.Period sub) {
        if (date == null|| sub ==null) return null;
        LocalDate date1 = stringToDate(date);
       return dateToString(date1.minus(sub));
    }
    public static Byte booleanToTinyint(Boolean b) {
        if (b == null) return null;
        return (byte) (b ? 1 : 0);
    }

    public static Byte smallintToTinyint(Short b) {
        if (b == null) return null;
        return b.byteValue();
    }

    public static Byte integerToTinyint(Long b) {
        if (b == null) return null;
        return b.byteValue();
    }

    public static Byte bigintToTinyint(Long b) {
        if (b == null) return null;
        return b.byteValue();
    }

    public static Byte decimalToTinyint(BigDecimal b) {
        if (b == null) return null;
        return b.byteValue();
    }

    public static Byte floatToTinyint(Double b) {
        if (b == null) return null;
        return b.byteValue();
    }

    public static Byte realToTinyint(Double b) {
        if (b == null) return null;
        return b.byteValue();
    }

    public static Byte doubleToTinyint(Double b) {
        if (b == null) return null;
        return b.byteValue();
    }

    public static Byte dateToTinyint(LocalDate b) {
        if (b == null) return null;
        return dateToBigint(b).byteValue();
    }

    public static Byte timeToTinyint(Duration b) {
        if (b == null) return null;
        return timeToBigint(b).byteValue();
    }

    public static Byte timestampToTinyint(LocalDateTime b) {
        if (b == null) return null;
        return Double.valueOf(timestampToDouble(b)).byteValue();
    }

    public static Byte periodToTinyint(Period b) {
        if (b == null) return null;
        return periodToLong(b).byteValue();
    }

    public static Byte durationToTinyint(Duration b) {
        if (b == null) return null;
        return durationToLong(b).byteValue();
    }

    public static Byte stringToTinyint(String b) {
        if (b == null) return null;
        return Byte.parseByte(b);
    }

    public static Byte byteStringToTinyint(ByteString b) {
        if (b == null) return null;
        byte[] bytes = b.getBytes();
        if (bytes.length > 0) {
            return bytes[0];
        }
        return 0;
    }

    public static Short tinyintToSmallint(Byte b) {
        if (b == null) return null;
        return b.shortValue();
    }

    public static Short integerToTSmallint(Long b) {
        if (b == null) return null;
        return b.shortValue();
    }

    public static Short bigintToSmallint(Long b) {
        if (b == null) return null;
        return b.shortValue();
    }

    public static Short bigDecimalToSmallint(BigDecimal b) {
        if (b == null) return null;
        return b.shortValue();
    }

    public static Short floatToSmallint(Double b) {
        if (b == null) return null;
        return b.shortValue();
    }

    public static Short realToSmallint(Float b) {
        if (b == null) return null;
        return b.shortValue();
    }

    public static Short doubleToSmallint(Double b) {
        if (b == null) return null;
        return b.shortValue();
    }

    public static Short dateToSmallint(LocalDate b) {
        if (b == null) return null;
        return dateToLong(b).shortValue();
    }

    public static Short timeToSmallint(Duration b) {
        if (b == null) return null;
        return timeToLong(b).shortValue();
    }

    public static Short timestampToSmallint(LocalDateTime b) {
        if (b == null) return null;
        return Double.valueOf(timestampToDouble(b)).shortValue();
    }

    public static Short periodToSmallint(Period b) {
        if (b == null) return null;
        return periodToLong(b).shortValue();
    }

    public static Short stringToSmallint(String b) {
        if (b == null) return null;
        return Short.parseShort(b);
    }

    public static Short byteStringToSmallint(ByteString b) {
        if (b == null) return null;
        if (b.length() > 0) {
            return (short) b.byteAt(0);
        }
        return 0;
    }

    public static Short durationToSmallint(Duration b) {
        if (b == null) return null;
        return durationToLong(b).shortValue();
    }

    public static Long booleanToBigint(Boolean b) {
        if (b == null) return null;
        return (long) (b ? 1 : 0);
    }

    public static Long tinyToBigint(Byte b) {
        if (b == null) return null;
        return b.longValue();
    }

    public static Long integerToBigint(Integer b) {
        if (b == null) return null;
        return b.longValue();
    }

    public static Long bigintToBigint(Long b) {
        if (b == null) return null;
        return b;
    }

    public static Long decimalToBigint(BigDecimal b) {
        if (b == null) return null;
        return b.longValue();
    }

    public static Long floatToBigint(Double b) {
        if (b == null) return null;
        return b.longValue();
    }

    public static Long realToBigint(Float b) {
        if (b == null) return null;
        return b.longValue();
    }

    public static Long doubleToBigint(Double b) {
        if (b == null) return null;
        return b.longValue();
    }

    public static Long timestampToBigint(LocalDateTime b) {
        if (b == null) return null;
        return Double.valueOf(timestampToDouble(b)).longValue();
    }

    public static Long periodToBigint(Period b) {
        if (b == null) return null;
        return periodToLong(b);
    }

    public static Long durationToBigint(Duration b) {
        if (b == null) return null;
        return durationToLong(b);
    }

    public static Long stringToBigint(String b) {
        if (b == null) return null;
        return Long.parseLong(b);
    }

    public static Long byteStringToBigint(ByteString b) {
        if (b == null) return null;
        if (b.length() > 0) {
            return (long) b.byteAt(0);
        }
        return 0L;
    }

    public static BigDecimal booleanToDecimal(Boolean b) {
        if (b == null) return null;
        return b ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    public static BigDecimal tinyintToDecimal(Byte b) {
        if (b == null) return null;
        return BigDecimal.valueOf(b);
    }

    public static BigDecimal integerToDecimal(Long b) {
        if (b == null) return null;
        return BigDecimal.valueOf(b);
    }

    public static BigDecimal bigintToDecimal(Long b) {
        if (b == null) return null;
        return BigDecimal.valueOf(b);
    }

    public static BigDecimal decimalToDecimal(BigDecimal b) {
        if (b == null) return null;
        return b;
    }

    public static BigDecimal floatToDecimal(Double b) {
        if (b == null) return null;
        return BigDecimal.valueOf(b);
    }

    public static BigDecimal realToDecimal(Float b) {
        if (b == null) return null;
        return BigDecimal.valueOf(b);
    }

    public static BigDecimal doubleToDecimal(Double b) {
        if (b == null) return null;
        return BigDecimal.valueOf(b);
    }

    public static BigDecimal timeToDecimal(Duration b) {
        if (b == null) return null;
        return BigDecimal.valueOf(timeToLong(b));
    }

    public static BigDecimal timestampToDecimal(LocalDateTime b) {
        if (b == null) return null;
        return BigDecimal.valueOf(timestampToDouble(b));
    }

    public static BigDecimal periodToDecimal(Period b) {
        if (b == null) return null;
        return BigDecimal.valueOf(periodToLong(b));
    }

    public static BigDecimal durationToDecimal(Duration b) {
        if (b == null) return null;
        return BigDecimal.valueOf(durationToDouble(b));
    }

    public static BigDecimal stringToDecimal(String b) {
        if (b == null) return null;
        return new BigDecimal(b);
    }

    public static BigDecimal bytestringToDecimal(ByteString b) {
        if (b == null) return null;
        return new BigDecimal(new String(b.getBytes()));
    }

    public static Double booleanToFloat(Boolean b) {
        if (b == null) return null;
        return b ? 1d : 0d;
    }

    public static Double tinyintToFloat(Byte b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double integerToFloat(Long b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double bigintToFloat(Long b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double decimalToFloat(BigDecimal b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double floatToFloat(Float b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double doubleToFloat(Double b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double dateDouble(LocalDate b) {
        if (b == null) return null;
        return dateToLong(b).doubleValue();
    }

    public static Double timeToDouble(Duration b) {
        if (b == null) return null;
        return durationToDouble(b);
    }

    //    public static Double timestampToDouble(LocalDateTime b){
//        if (b==null)return null;
//        return timestampToDouble(b);
//    }
    public static Double periodToDouble(Period b) {
        if (b == null) return null;
        return periodToLong(b).doubleValue();
    }

    //    public static Double durationToDouble(Duration b){
//        if (b==null)return null;
//        return durationToDouble(b);
//    }
    public static Double stringToDouble(String b) {
        if (b == null) return null;
        return Double.parseDouble(b);
    }

    public static Double byteStringToDouble(ByteString b) {
        if (b == null) return null;
        return Double.parseDouble(new String(b.getBytes()));
    }

    /////////////////////////////////////////////////////
    public static Double booleanToDouble(Boolean b) {
        if (b == null) return null;
        return b ? 1d : 0d;
    }

    public static Double tinyintToDouble(Byte b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double smallintToDouble(Long b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double integerToDouble(Long b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double bigintToDouble(Long b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double decimalToDouble(BigDecimal b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double floatToDouble(Double b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double realToDouble(Float b) {
        if (b == null) return null;
        return b.doubleValue();
    }

    public static Double dateToFloat(LocalDate b) {
        if (b == null) return null;
        return dateToLong(b).doubleValue();
    }

    public static Double timeToFloat(Duration b) {
        if (b == null) return null;
        return durationToDouble(b);
    }

    public static Double timestampToFloat(LocalDateTime b) {
        if (b == null) return null;
        return timestampToDouble(b);
    }

    public static Double periodToFloat(Period b) {
        if (b == null) return null;
        return periodToLong(b).doubleValue();
    }

    public static Double durationToFloat(Duration b) {
        if (b == null) return null;
        return durationToDouble(b);
    }

    public static Double stringToFloat(String b) {
        if (b == null) return null;
        return Double.parseDouble(b);
    }

    public static Double byteStringToFloat(ByteString b) {
        if (b == null) return null;
        return Double.parseDouble(new String(b.getBytes()));
    }

    /////////////////////////////////////////
    public static Float booleanToReal(Boolean b) {
        if (b == null) return null;
        return b ? 1f : 0f;
    }

    public static Float tinyintToReal(Byte b) {
        if (b == null) return null;
        return b.floatValue();
    }

    public static Float smallintToReal(Short b) {
        if (b == null) return null;
        return b.floatValue();
    }

    public static Float integerToReal(Long b) {
        if (b == null) return null;
        return b.floatValue();
    }

    public static Float bigintToReal(Long b) {
        if (b == null) return null;
        return b.floatValue();
    }

    public static Float decimalToReal(BigDecimal b) {
        if (b == null) return null;
        return b.floatValue();
    }

    public static Float floatToReal(Double b) {
        if (b == null) return null;
        return b.floatValue();
    }

    public static Float realToReal(Double b) {
        if (b == null) return null;
        return b.floatValue();
    }

    public static Float doubleToReal(LocalDate b) {
        if (b == null) return null;
        return dateToLong(b).floatValue();
    }

    public static Float dateToReal(LocalDate b) {
        if (b == null) return null;
        return dateToLong(b).floatValue();
    }

    public static Float timeToReal(Duration b) {
        if (b == null) return null;
        return durationToDouble(b).floatValue();
    }

    public static Float timestampToReal(LocalDateTime b) {
        if (b == null) return null;
        return Double.valueOf(timestampToDouble(b)).floatValue();
    }

    public static Float periodToReal(Period b) {
        if (b == null) return null;
        return periodToLong(b).floatValue();
    }

    public static Float durationToReal(Duration b) {
        if (b == null) return null;
        return durationToDouble(b).floatValue();
    }

    public static Float stringToReal(String b) {
        if (b == null) return null;
        return Float.parseFloat(b);
    }

    public static Float byteStringToReal(ByteString b) {
        if (b == null) return null;
        return Float.parseFloat(new String(b.getBytes()));
    }

    ///////////////////////////////////////////////////
    public static LocalDate booleanToDate(Boolean b) {
        if (b == null) return null;
        return doubleToDate(b ? 1d : 0d);
    }

    public static LocalDate tinyintToDate(Byte b) {
        if (b == null) return null;

        return doubleToDate(b.doubleValue());
    }

    public static LocalDate smallintToDate(Short b) {
        if (b == null) return null;
        return doubleToDate(b.doubleValue());
    }

    public static LocalDate integerToDate(Long b) {
        if (b == null) return null;
        return doubleToDate(b.doubleValue());
    }

    public static LocalDate bigintToDate(Long b) {
        if (b == null) return null;
        return doubleToDate(b.doubleValue());
    }

    public static LocalDate decimalToDate(BigDecimal b) {
        if (b == null) return null;
        return doubleToDate(b.doubleValue());
    }

    public static LocalDate floatToDate(Double b) {
        if (b == null) return null;
        return doubleToDate(b.doubleValue());
    }

    public static LocalDate realToDate(Float b) {
        if (b == null) return null;
        return doubleToDate(b.doubleValue());
    }

    public static LocalDate doubleToDate(Double x) {
        if (x == null) return null;
        //2003_1230
        int year = (int) (x / 1000L);
        int month = (int) ((x - year) / 10L);
        int days = (int) ((x - year - month));
        return LocalDate.of(year, month, days);
    }

    public static LocalDate timestampToDate(LocalDateTime b) {
        if (b == null) return null;
        return b.toLocalDate();
    }

    public static LocalDate periodToDate(Period b) {
        if (b == null) return null;
        return LocalDate.of(b.getYears(), b.getMonths(), b.getDays());
    }

    public static LocalDate durationToDate(Duration b) {
        if (b == null) return null;
        return LocalDate.of(0, 1, 1).plus(b);
    }

    public static LocalDate byteStringToDate(ByteString b) {
        if (b == null) return null;
        return stringToDate(new String(b.getBytes()));
    }


    public static LocalDate timeToDate(Duration b) {
        if (b == null) return null;
        return LocalDate.of(0, 0, (int) b.toDays());
    }

    public static LocalDate stringToDate(String b) {
        if (b == null) return null;
        return dateStringToDate(b);
    }

    //////////////////////////////////////////
    public static LocalDateTime booleanToTimestamp(Boolean b) {
        if (b == null) return null;
        int i = b ? 1 : 0;
        return LocalDateTime.ofEpochSecond(i, 0, ZoneOffset.UTC);
    }

    public static LocalDateTime tinyintToTimestamp(Byte b) {
        if (b == null) return null;
        return doubleToTimestamp(b.doubleValue());
    }

    public static LocalDateTime smallintToTimestamp(Short b) {
        if (b == null) return null;
        return doubleToTimestamp(b.doubleValue());
    }

    public static LocalDateTime integerToTimestamp(Long b) {
        if (b == null) return null;
        return doubleToTimestamp(b.doubleValue());
    }

    public static LocalDateTime bigintToTimestamp(Long b) {
        if (b == null) return null;
        return doubleToTimestamp(b.doubleValue());
    }

    public static LocalDateTime decimalToTimestamp(BigDecimal b) {
        if (b == null) return null;
        return doubleToTimestamp(b.doubleValue());
    }

    public static LocalDateTime floatToTimestamp(Double b) {
        if (b == null) return null;
        return doubleToTimestamp(b);
    }

    public static LocalDateTime realToTimestamp(Float b) {
        if (b == null) return null;
        return doubleToTimestamp(b.doubleValue());
    }

    public static LocalDateTime doubleToTimestamp(Double b) {
        if (b == null) return null;
        double x = b - Math.floor(b);
        //2003_1230_1230
        int year = (int) (b / 1000_0000_0000L);
        int month = (int) ((x - year) / 100_0000L);
        int days = (int) ((x - year - month) / 10000L);
        int hours = (int) ((x - year - month - days) / 100L);
        int minutes = (int) ((x - year - month - days - hours) / 100L);
        return LocalDateTime.of(year, month, days, hours, minutes).withNano((int) (x * Math.pow(10, 9)));
    }

    public static LocalDateTime dateToTimestamp(LocalDate b) {
        if (b == null) return null;
        return b.atStartOfDay();
    }

    public static LocalDateTime timeToTimestamp(Duration b) {
        if (b == null) return null;
        return LocalDateTime.of(LocalDate.ofYearDay(0,1),
                LocalTime.ofSecondOfDay(b.getSeconds()).withNano(b.getNano()));
    }

    public static LocalDateTime timestampToTimestamp(LocalDateTime b) {
        if (b == null) return null;
        return b;
    }

    public static LocalDateTime periodToTimestamp(Period b) {
        if (b == null) return null;
        return LocalDate.of(b.getYears(), b.getMonths(), b.getDays()).atStartOfDay();
    }

    public static LocalDateTime byteStringToTimestamp(ByteString b) {
        if (b == null) return null;
        return stringToTimestamp(new String(b.getBytes()));
    }

    public static LocalDateTime stringToTimestamp(String b) {
        if (b == null) return null;
        Temporal temporal = timestampStringToTimestamp(b);
        if (temporal instanceof LocalDateTime) {
            return (LocalDateTime) temporal;
        }
        if (temporal instanceof LocalDate) {
            return ((LocalDate) temporal).atStartOfDay();
        }
        return null;
    }

    public static Boolean stringToBoolean(String b) {
        if (b == null) return null;
        b = b.trim();
        if (b.length() == 1) {
            char c = b.charAt(0);
            switch (c) {
                case '1':
                    return true;
                case '0':
                    return false;
                default:
            }
        }
        return Boolean.parseBoolean(b);
    }

    public static Boolean booleanToBoolean(Boolean b) {
        if (b == null) return null;
        return b;
    }

    public static Boolean tinyintToBoolean(Byte b) {
        if (b == null) return null;
        return b > 0;
    }

    public static Boolean smallintToBoolean(Short b) {
        if (b == null) return null;
        return b > 0;
    }

    public static Boolean integerToBoolean(Long b) {
        if (b == null) return null;
        return b > 0;
    }

    public static Boolean bigintToBoolean(Long b) {
        if (b == null) return null;
        return b > 0;
    }

    public static Boolean decimalToBoolean(BigDecimal b) {
        if (b == null) return null;
        return b.compareTo(BigDecimal.ZERO) > 0;
    }

    public static Boolean floatToBoolean(Double b) {
        if (b == null) return null;
        return b > 0;
    }

    public static Boolean realToBoolean(Float b) {
        if (b == null) return null;
        return b > 0;
    }

    public static Boolean doubleToBoolean(Double b) {
        if (b == null) return null;
        return b > 0;
    }

    public static Boolean dateToBoolean(LocalDate b) {
        if (b == null) return null;
        return dateToLong(b) > 0;
    }

    public static Boolean timeToBoolean(Duration b) {
        if (b == null) return null;
        return b.compareTo(Duration.ZERO) > 0;
    }

    public static Boolean periodToBoolean(Period b) {
        if (b == null) return null;
        return !b.isZero();
    }

    public static Boolean timestampToBoolean(LocalDateTime b) {
        if (b == null) return null;
        return timestampToDouble(b) > 0;
    }

    public static Boolean byteStringToBoolean(ByteString b) {
        if (b == null) return null;
        return stringToBoolean(new String(b.getBytes()));
    }

    public static Period stringToPeriod(String b) {
        if (b == null) return null;
        LocalDate localDate = dateStringToDate(b);
        return Period.between(
                LocalDate.of(0, 0, 1), localDate);
    }

    public static Period booleanToPeriod(Boolean b) {
        if (b == null) return null;
        return Period.ofDays(b ? 1 : 0);
    }

    public static Period tinyintToPeriod(Byte b) {
        if (b == null) return null;
        return Period.ofDays(b);
    }

    public static Period smallintToPeriod(Short b) {
        if (b == null) return null;
        return Period.ofDays(b);
    }

    public static Period integerToPeriod(Long b) {
        if (b == null) return null;
        return Period.ofDays(b.intValue());
    }

    public static Period bigintToPeriod(Long b) {
        if (b == null) return null;
        return Period.ofDays(b.intValue());
    }

    public static Period decimalToPeriod(BigDecimal b) {
        if (b == null) return null;
        return Period.ofDays(b.intValue());
    }

    public static Period floatToPeriod(Double b) {
        if (b == null) return null;
        return Period.ofDays(b.intValue());
    }

    public static Period realToPeriod(Float b) {
        if (b == null) return null;
        return Period.ofDays(b.intValue());
    }

    public static Period doubleToPeriod(Double b) {
        if (b == null) return null;
        return Period.ofDays(b.intValue());
    }

    public static Period dateToPeriod(LocalDate b) {
        if (b == null) return null;
        return Period.between(LocalDate.of(0, 0, 1),
                b);
    }

    public static Period timeToPeriod(Duration b) {
        if (b == null) return null;
       throw new UnsupportedOperationException();
    }

    public static Period timestampToPeriod(LocalDateTime b) {
        if (b == null) return null;
        return Period.between(LocalDate.of(0, 0, 1),
                b.toLocalDate());
    }

    public static Period periodToPeriod(Period b) {
        if (b == null) return null;
        return b;
    }

    public static Period byteStringToPeriod(ByteString b) {
        if (b == null) return null;
        return stringToPeriod(new String(b.getBytes()));
    }

    public static Duration stringToDuration(String b) {
        if (b == null) return null;
        return timeStringToTimeDuration(b);
    }

    public static Duration booleanToDuration(Boolean b) {
        if (b == null) return null;
        return Duration.ofSeconds(b ? 1 : 0);
    }

    public static Duration tinyintToDuration(Byte b) {
        if (b == null) return null;
        return doubleToDuration(b.doubleValue());
    }

    public static Duration smallintToDuration(Short b) {
        if (b == null) return null;
        return doubleToDuration(b.doubleValue());
    }

    public static Duration integerToDuration(Long b) {
        if (b == null) return null;
        return doubleToDuration(b.doubleValue());
    }

    public static Duration bigintToDuration(Long b) {
        if (b == null) return null;
        return doubleToDuration(b.doubleValue());
    }

    public static Duration decimalToDuration(BigDecimal b) {
        if (b == null) return null;
        return doubleToDuration(b.doubleValue());
    }

    public static Duration floatToDuration(Double b) {
        if (b == null) return null;
        return doubleToDuration(b);
    }

    public static Duration realToDuration(Float b) {
        if (b == null) return null;
        return doubleToDuration(b.doubleValue());
    }

    public static Duration doubleToDuration(Double b) {
        if (b == null) return null;
        double x = b - Math.floor(b);
        int nanos = (int) (x * Math.pow(10, 9));
        return Duration.ofSeconds(b.longValue()).withNanos(nanos);
    }

    public static Duration dateToDuration(LocalDate b) {
        if (b == null) return null;
        return timestampToDuration(b.atStartOfDay());
    }

    public static Duration timeToDuration(Duration b) {
        if (b == null) return null;
        return b;
    }

    public static Duration timestampToDuration(LocalDateTime b) {
        if (b == null) return null;
        return Duration
                .between(
                        LocalDateTime.of(0, 1, 1, 0, 0),
                        b);
    }

    public static Duration periodToDuration(Period b) {
        if (b == null) return null;
        Long aLong = periodToLong(b);

        return Duration.ofDays(aLong);
    }

    public static Duration byteStringToDuration(ByteString b) {
        if (b == null) return null;
        return stringToDuration(new String(b.getBytes()));
    }

    public static String stringToString(String b) {
        if (b == null) return null;
        return b;
    }

    public static String booleanToString(Boolean b) {
        if (b == null) return null;
        return b ? "1" : "0";
    }

    public static String smallintToString(Short b) {
        if (b == null) return null;
        return b.toString();
    }

    public static String integerToString(Long b) {
        if (b == null) return null;
        return b.toString();
    }

    public static String bigintToString(Long b) {
        if (b == null) return null;
        return b.toString();
    }

    public static String decimalToString(BigDecimal b) {
        if (b == null) return null;
        return b.toString();
    }

    public static String floatToString(Double b) {
        if (b == null) return null;
        return b.toString();
    }

    public static String realToString(Float b) {
        if (b == null) return null;
        return b.toString();
    }

    public static String doubleToString(Double b) {
        if (b == null) return null;
        return b.toString();
    }

    public static String dateToString(LocalDate b) {
        if (b == null) return null;
        return String.format("%04d-%02d-%02d",
                b.getYear(),
                b.getMonthValue(),
                b.getDayOfMonth());
    }

    public static String timeToString(Duration b) {
        if (b == null) return null;
        long seconds = b.getSeconds();
        int SECONDS_PER_HOUR = 60 * 60;
        int SECONDS_PER_MINUTE = 60;
        long hours = seconds / SECONDS_PER_HOUR;
        int minutes = (int) ((seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
        int secs = (int) (seconds % SECONDS_PER_MINUTE);
        if (b.getNano() == 0) {
            return String.format("%02d:%02d:%02d",
                    hours,
                    minutes,
                    secs);
        }
        return String.format("%02d:%02d:%02d.%09d",
                hours,
                minutes,
                secs,
                (long) (b.getNano() * Math.pow(10, 9)));
    }

    public static String timestampToString(LocalDateTime b) {
        if (b == null) return null;
        String f = dateToString(b.toLocalDate());
        LocalTime localTime = b.toLocalTime();
        int hour = localTime.getHour();
        int minute = localTime.getMinute();
        int second = localTime.getSecond();
        int nano = localTime.getNano();
        if (b.getNano() == 0) {
            return f + " " + String.format("%02d:%02d:%02d",
                    hour,
                    minute,
                    second);
        }
        return f + " " + String.format("%02d:%02d:%02d.%09d",
                hour,
                minute,
                second,
                (long) (b.getNano() * Math.pow(10, 9)));
    }

    public static String periodToString(Period b) {
        if (b == null) return null;
        return dateToString(LocalDate.of(b.getYears(),
                b.getMonths(),
                b.getDays()));
    }

    public static String byteStringToString(ByteString b) {
        if (b == null) return null;
        return new String(b.getBytes());
    }

    public static Double doubleToDouble(Double b) {
        if (b == null) return null;
        return b;
    }
    public static Double dateToDouble(LocalDate b) {
        if (b == null) return null;
        return dateToLong(b).doubleValue();
    }
    public static LocalDate dateToDate(LocalDate b) {
        if (b == null) return null;
        return b;
    }
//    public static LocalDate bigintToDate(Long b) {
//        if (b == null) return null;
//        return b;
//    }
    public static Long durationToLong(Duration b) {
        if (b == null) return null;
        long seconds = b.getSeconds();
        final int SECONDS_PER_HOUR = 60 * 60;
        final int SECONDS_PER_MINUTE = 60;
        long hours = seconds / SECONDS_PER_HOUR;
        int minutes = (int) ((seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
        int secs = (int) (seconds % SECONDS_PER_MINUTE);

        return hours * 10000 + minutes * 100 + secs;
    }

    public static Double durationToDouble(Duration b) {
        if (b == null) return null;
        long seconds = b.getSeconds();
        final int SECONDS_PER_HOUR = 60 * 60;
        final int SECONDS_PER_MINUTE = 60;
        long hours = seconds / SECONDS_PER_HOUR;
        int minutes = (int) ((seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
        int secs = (int) (seconds % SECONDS_PER_MINUTE);

        return hours * 10000 + minutes * 100 + secs + 0.000000001 * b.getNano();
    }

    public static Long periodToLong(Period b) {
        if (b == null) return null;
        int years = b.getYears();
        int months = b.getMonths();
        long days = b.getDays();
        return years * 10000 + months * 1000 + days;
    }

    public static Long timeToBigint(Duration b) {
        if (b == null) return null;
        return timeToLong(b);
    }

    public static Long timeToLong(Duration b) {
        return durationToLong(b);
    }

    public static Long dateToBigint(LocalDate b) {
        if (b == null) return null;
        return dateToLong(b);
    }

    public static double timestampToDouble(LocalDateTime localDateTime) {
        return Double.parseDouble(localDateTime.format(new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4)
                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .appendLiteral('.')
                .appendValue(ChronoField.MICRO_OF_SECOND)
                .toFormatter()));
    }

    public static long extract(TimeUnitRange unit, String dateTimeText) {
        Object o = MycatBuiltInMethodImpl.parseUnknownDateTimeText(dateTimeText);
        if (o instanceof Temporal) {
            Temporal temporal = (Temporal) o;
            return extract(unit, temporal);
        } else if (o instanceof Duration) {
            Duration duration = (Duration) o;
            int days = (int) duration.toDays();
            if (days == 0) {
                LocalTime plus = LocalTime.ofNanoOfDay(0).plus(duration);
                return extract(unit, plus);
            }
            LocalDateTime plus = LocalDateTime.of(1, 1, days, 0, 0)
                    .plus(duration.minusDays(1));
            return extract(unit, plus);
        }
        throw new UnsupportedOperationException();
    }

    public static Long dateToLong(LocalDate tmp) {
        if (tmp == null) {
            return null;
        }
        long year = tmp.getYear();
        int month = tmp.getMonth().getValue();
        int dayOfMonth = tmp.getDayOfMonth();
        return Long.parseLong(String.format("%04d%02d%02d", year, month, dayOfMonth));
    }

    public static LocalDate longToDate(Long tmp) {
        long time = DateParserUtils.parseDate(Long.toString(tmp)).getTime();
        return LocalDate.ofEpochDay(time);
    }

    public static LocalDateTime longToTimestamp(Long tmp) {
        LocalDateTime localDateTime = DateParserUtils.parseDateTime(Long.toString(tmp));
        return localDateTime;
    }

    public static Duration stringToTime(String tmp) {
        return stringToDuration(tmp);
    }

    public static Byte realToTinyint(Float tmp) {
        if (tmp == null) return null;
        return tmp.byteValue();
    }

    public static Short booleanToSmallint(Boolean tmp) {
        if (tmp == null) return null;
        return (short) (tmp ? 1 : 0);
    }

    public static Long integerToBigint(Long tmp) {
        if (tmp == null) return null;
        return tmp;
    }

    public static Double floatToFloat(Double tmp) {
        if (tmp == null) return null;
        return tmp;
    }

    public static Double realToFloat(Float tmp) {
        if (tmp == null) return null;
        return tmp.doubleValue();
    }

    public static Float realToReal(Float tmp) {
        if (tmp == null) return null;
        return tmp.floatValue();
    }

    public static Float doubleToReal(Double tmp) {
        if (tmp == null) return null;
        return tmp.floatValue();
    }

    public static Double smallintToDouble(Short tmp) {
        if (tmp == null) return null;
        return tmp.doubleValue();
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
        Temporal temporal = timestampStringToTimestamp(s);
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
        Temporal temporal = timestampStringToTimestamp(s);
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
        if (s.contains(":") && !s.contains("-")) {
            String[] split = s.split(":");
            return Duration.ofHours(Long.parseLong(split[split.length - 3]))
                    .plusMinutes(Long.parseLong(split[split.length - 2]))
                    .plusSeconds(Long.parseLong(split[split.length - 1]));
        }
        return timestampStringToTimestamp(s);
    }

    public static Temporal timestampStringToTimestamp(String s) {
       return MycatTimeUtil.timestampStringToTimestamp(s);
    }

    public static Temporal timestampStringToDate(String s) {
        Temporal temporal = timestampStringToTimestamp(s);
        if (temporal instanceof LocalDate) {
            return temporal;
        }
        if (temporal instanceof LocalDateTime) {
            return ((LocalDateTime) temporal).toLocalDate();
        }
        throw new UnsupportedOperationException();
    }


    public static Duration timeStringToDate(String s) {
        return timeStringToTimeDuration(s);
    }

    public static Duration timeStringToTimeDuration(String text) {
     return    MycatTimeUtil.timeStringToTimeDuration(text);
    }

    /**
     * SELECT LAST_DAY('2003-03-32'); unsupported
     *
     * @param s
     * @return
     */
    public static LocalDate dateStringToDate(String s) {
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
        } catch (Throwable e) {
            return null;
        }
    }

}

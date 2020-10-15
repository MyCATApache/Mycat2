package org.apache.calcite.mycat;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.runtime.SqlFunctions;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.TimeZone;

public enum  MycatBuiltInMethod {
//    INTERNAL_TO_DATE(MycatBuiltInMethodImpl.class, "internalToDate", int.class),
//    INTERNAL_TO_TIME(MycatBuiltInMethodImpl.class, "internalToTime", int.class),
//    INTERNAL_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "internalToTimestamp", long.class),
    STRING_TO_DATE(MycatBuiltInMethodImpl.class, "dateStringToUnixDate", String.class),
    STRING_TO_TIME(MycatBuiltInMethodImpl.class, "timeStringToUnixDate", String.class),
    STRING_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "timestampStringToUnixTimestamp", String.class),
//    INTEGER_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "intToUnixTimestamp", Long.class),
    LONG_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "longToUnixTimestamp", Long.class),
    LONG_TO_DATE(MycatBuiltInMethodImpl.class, "longToUnixDate", Long.class),
    DATE_TO_LONG(MycatBuiltInMethodImpl.class, "dateToLong", LocalDate.class),
    TIMESTAMP_TO_DOUBLE(MycatBuiltInMethodImpl.class, "timestampToDouble", LocalDateTime.class),
//    STRING_TO_TIME_WITH_LOCAL_TIME_ZONE(MycatBuiltInMethodImpl.class, "toTimeWithLocalTimeZone",
//            String.class),
//    TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE(MycatBuiltInMethodImpl.class, "toTimeWithLocalTimeZone",
//            String.class, TimeZone.class),
//    STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(MycatBuiltInMethodImpl.class, "toTimestampWithLocalTimeZone",
//            String.class),
//    TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(MycatBuiltInMethodImpl.class,
//            "toTimestampWithLocalTimeZone", String.class, TimeZone.class),
//    TIME_WITH_LOCAL_TIME_ZONE_TO_TIME(MycatBuiltInMethodImpl.class, "timeWithLocalTimeZoneToTime",
//            int.class, TimeZone.class),
//    TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP(MycatBuiltInMethodImpl.class, "timeWithLocalTimeZoneToTimestamp",
//            String.class, int.class, TimeZone.class),
//    TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(MycatBuiltInMethodImpl.class,
//            "timeWithLocalTimeZoneToTimestampWithLocalTimeZone", String.class, int.class),
//    TIME_WITH_LOCAL_TIME_ZONE_TO_STRING(MycatBuiltInMethodImpl.class, "timeWithLocalTimeZoneToString",
//            int.class, TimeZone.class),
//    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE(MycatBuiltInMethodImpl.class, "timestampWithLocalTimeZoneToDate",
//            long.class, TimeZone.class),
//    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME(MycatBuiltInMethodImpl.class, "timestampWithLocalTimeZoneToTime",
//            long.class, TimeZone.class),
//    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE(SqlFunctions.class,
//            "timestampWithLocalTimeZoneToTimeWithLocalTimeZone", long.class),
//    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP(MycatBuiltInMethodImpl.class,
//            "timestampWithLocalTimeZoneToTimestamp", long.class, TimeZone.class),
//    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING(MycatBuiltInMethodImpl.class,
//            "timestampWithLocalTimeZoneToString", long.class, TimeZone.class),
//    UNIX_DATE_TO_STRING(MycatBuiltInMethodImpl.class, "unixDateToString", int.class),
//    UNIX_TIME_TO_STRING(MycatBuiltInMethodImpl.class, "unixTimeToString", int.class),
//    UNIX_TIMESTAMP_TO_STRING(MycatBuiltInMethodImpl.class, "unixTimestampToString",
//            long.class),
//    INTERVAL_YEAR_MONTH_TO_STRING(MycatBuiltInMethodImpl.class,
//            "intervalYearMonthToString", int.class, TimeUnitRange.class),
//    INTERVAL_DAY_TIME_TO_STRING(MycatBuiltInMethodImpl.class, "intervalDayTimeToString",
//            long.class, TimeUnitRange.class, int.class),
//    UNIX_DATE_EXTRACT(MycatBuiltInMethodImpl.class, "unixDateExtract",
//            TimeUnitRange.class, long.class),
//    UNIX_DATE_FLOOR(MycatBuiltInMethodImpl.class, "unixDateFloor",
//            TimeUnitRange.class, int.class),
//    UNIX_DATE_CEIL(MycatBuiltInMethodImpl.class, "unixDateCeil",
//            TimeUnitRange.class, int.class),
//    UNIX_TIMESTAMP_FLOOR(MycatBuiltInMethodImpl.class, "unixTimestampFloor",
//            TimeUnitRange.class, long.class),
//    UNIX_TIMESTAMP_CEIL(MycatBuiltInMethodImpl.class, "unixTimestampCeil",
//            TimeUnitRange.class, long.class),
//    LAST_DAY(MycatBuiltInMethodImpl.class, "lastDay", int.class),
//    DAYNAME_WITH_TIMESTAMP(MycatBuiltInMethodImpl.class,
//            "dayNameWithTimestamp", long.class, Locale.class),
//    DAYNAME_WITH_DATE(MycatBuiltInMethodImpl.class,
//            "dayNameWithDate", int.class, Locale.class),
//    MONTHNAME_WITH_TIMESTAMP(MycatBuiltInMethodImpl.class,
//            "monthNameWithTimestamp", long.class, Locale.class),
//    MONTHNAME_WITH_DATE(MycatBuiltInMethodImpl.class,
//            "monthNameWithDate", int.class, Locale.class),
    ;
    public final Method method;
    public final Constructor constructor;
    public final Field field;

    MycatBuiltInMethod(Method method, Constructor constructor, Field field) {
        this.method = method;
        this.constructor = constructor;
        this.field = field;
    }

    /** Defines a method. */
    MycatBuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
        this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
    }

    /** Defines a constructor. */
    MycatBuiltInMethod(Class clazz, Class... argumentTypes) {
        this(null, Types.lookupConstructor(clazz, argumentTypes), null);
    }

    /** Defines a field. */
    MycatBuiltInMethod(Class clazz, String fieldName, boolean dummy) {
        this(null, null, Types.lookupField(clazz, fieldName));
        assert dummy : "dummy value for method overloading must be true";
    }
}

package io.mycat.hbt;

public enum  HBTTypes {
    Bool,
    Tinyint,
    Smallint,
    Integer,
    Bigint,
    Decimal,
    Float,
    Real,
    Double,
    Date,
    Time,
    TimeWithLocalTimeZone,
    Timestamp,
    TimestampWithLocalTimeZone,
    IntervalYear,
    IntervalYearMonth,
    IntervalMonth,
    IntervalDay,
    IntervalDayHour,
    IntervalDayMinute,
    IntervalDaySecond,
    IntervalHour,
    IntervalHourMinute,
    IntervalMinute,
    IntervalMinuteSecond,
    IntervalSecond,
    Char,
    Varchar,
    Binary,
    Varbinary,
    Null,
    ;
    public String getName(){
        return name().toLowerCase();
    }
}
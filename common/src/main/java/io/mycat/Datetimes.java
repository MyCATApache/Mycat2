/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.mycat;

import io.questdb.std.datetime.microtime.Timestamps;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class Datetimes {

    public static LocalDateTime toJavaLocalDateTime(long value) {
        final int year = Timestamps.getYear(value);
        final boolean isLeap = Timestamps.isLeapYear(year);
        final int month = Timestamps.getMonthOfYear(value, year, isLeap);
        final int dayOfMonth = Timestamps.getDayOfMonth(value, year, month, isLeap);
        final int hourOfDay = Timestamps.getHourOfDay(value);
        final int minuteOfHour = Timestamps.getMinuteOfHour(value);
        final int secondOfMinute = Timestamps.getSecondOfMinute(value);
        final int millisOfSecond = Timestamps.getMillisOfSecond(value);

        return LocalDateTime
                .of(year, month, dayOfMonth, hourOfDay,
                        minuteOfHour, secondOfMinute, millisOfSecond * 1000);
    }

    public static Timestamp toJavaTimestamp(long value) {
        LocalDateTime localDateTime = toJavaLocalDateTime(value);
        return Timestamp.valueOf(localDateTime);
    }

    public static String toMySQLResultDatetimeText(long value) {

        final int year = Timestamps.getYear(value);
        final boolean isLeap = Timestamps.isLeapYear(year);
        final int month = Timestamps.getMonthOfYear(value, year, isLeap);
        final int dayOfMonth = Timestamps.getDayOfMonth(value, year, month, isLeap);
        final int hour = Timestamps.getHourOfDay(value);
        final int minute = Timestamps.getMinuteOfHour(value);
        final int second = Timestamps.getSecondOfMinute(value);
        final int millisOfSecond = Timestamps.getMillisOfSecond(value);

        if (millisOfSecond == 0) {
            return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    year,
                    month,
                    dayOfMonth,
                    hour,
                    minute,
                    second
            );
        }
        return (String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",//"%04d-%02d-%02d %02d:%02d:%02d.%09d"
                year,
                month,
                dayOfMonth,
                hour,
                minute,
                second,
                millisOfSecond
        ));
    }

    public static Date toJavaDate(long value) {
        LocalDateTime localDateTime = toJavaLocalDateTime(value);
        return Date.valueOf(localDateTime.toLocalDate());
    }

    public static String toMySQLResultDateText(long value) {
        final int year = Timestamps.getYear(value);
        final boolean isLeap = Timestamps.isLeapYear(year);
        final int month = Timestamps.getMonthOfYear(value, year, isLeap);
        final int dayOfMonth = Timestamps.getDayOfMonth(value, year, month, isLeap);

        return String.format("%04d-%02d-%02d",
                year,
                month,
                dayOfMonth
        );
    }

    public static LocalDate toJavaLocalDate(long value) {
        final int year = Timestamps.getYear(value);
        final boolean isLeap = Timestamps.isLeapYear(year);
        final int month = Timestamps.getMonthOfYear(value, year, isLeap);
        int dayOfMonth = Timestamps.getDayOfMonth(value, year, month, isLeap);
        int hourOfDay = Timestamps.getHourOfDay(value);
        int minuteOfHour = Timestamps.getMinuteOfHour(value);
        int secondOfMinute = Timestamps.getSecondOfMinute(value);
        int millisOfSecond = Timestamps.getMillisOfSecond(value);

        LocalDate localDate = LocalDate.of(year, month, dayOfMonth);
        return localDate;
    }

    public static LocalTime toJavaLocalTime(long value) {
//        final int year = Timestamps.getYear(value);
//        final boolean isLeap = Timestamps.isLeapYear(year);
//        final int month = Timestamps.getMonthOfYear(value, year, isLeap);
//        int dayOfMonth = Timestamps.getDayOfMonth(value, year, month, isLeap);
//        int hourOfDay = Timestamps.getHourOfDay(value);
        int minuteOfHour = Timestamps.getMinuteOfHour(value);
        int secondOfMinute = Timestamps.getSecondOfMinute(value);
        int millisOfSecond = Timestamps.getMillisOfSecond(value);

        return LocalTime.of(minuteOfHour, secondOfMinute, millisOfSecond * 1000);
    }

    public static Time toJavaTime(long value) {
        LocalTime localTime = toJavaLocalTime(value);
        return Time.valueOf(localTime);
    }

    public static String toMySQLResultTimeText(long value) {
        int hour = Timestamps.getHourOfDay(value);
        int minute = Timestamps.getMinuteOfHour(value);
        int second = Timestamps.getSecondOfMinute(value);
        int millisOfSecond = Timestamps.getMillisOfSecond(value);
        if (millisOfSecond == 0) {
            return String.format("%02d:%02d:%02d", hour, minute, second);
        }
        return String.format("%02d:%02d:%02d.%09d", hour, minute, second, millisOfSecond);
    }

}

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

import io.mycat.util.DateUtil;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static com.mysql.cj.protocol.a.MysqlTextValueDecoder.TIME_STR_LEN_MAX_WITH_MICROS;

public class Datetimes {

    public static LocalDateTime toJavaLocalDateTime(long value) {
        Timestamp timestamp = new Timestamp(value);
//        final int year = DateUtil.getYear(timestamp);
//        final int month = DateUtil.getMonth(timestamp);
//        final int dayOfMonth =  DateUtil.getDay(timestamp);
//        final int hourOfDay =  DateUtil.getHour(timestamp);
//        final int minuteOfHour = DateUtil.getMinute(timestamp);
//        final int secondOfMinute =  DateUtil.getSecond(timestamp);
//        final int millisOfSecond =  DateUtil.getSecond(timestamp);

        return timestamp.toLocalDateTime();
    }

    public static Timestamp toJavaTimestamp(long value) {
        LocalDateTime localDateTime = toJavaLocalDateTime(value);
        return Timestamp.valueOf(localDateTime);
    }

    public static String toMySQLResultDatetimeText(long value) {

        Timestamp timestamp = new Timestamp(value);
        final int year =  DateUtil.getYear(timestamp);
        final int month = DateUtil.getMonth(timestamp);
        final int dayOfMonth = DateUtil.getDay(timestamp);
        final int hour = DateUtil.getHour(timestamp);
        final int minute = DateUtil.getMinute(timestamp);
        final int second =  DateUtil.getSecond(timestamp);
        final int millisOfSecond = DateUtil.getMicroSecond(timestamp);

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
        Date date = new Date(value);
        final int year =DateUtil.getYear(date);
        final int month = DateUtil.getMonth(date);
        final int dayOfMonth = DateUtil.getDay(date);

        return String.format("%04d-%02d-%02d",
                year,
                month,
                dayOfMonth
        );
    }

    public static LocalDate toJavaLocalDate(long value) {
        return new Date(value).toLocalDate();
    }

    public static LocalTime toJavaLocalTime(long value) {
//        final int year = Timestamps.getYear(value);
//        final boolean isLeap = Timestamps.isLeapYear(year);
//        final int month = Timestamps.getMonthOfYear(value, year, isLeap);
//        int dayOfMonth = Timestamps.getDayOfMonth(value, year, month, isLeap);
//        int hourOfDay = Timestamps.getHourOfDay(value);
        Time time = new Time(value);

        return time.toLocalTime();
    }

    public static Time toJavaTime(long value) {
        LocalTime localTime = toJavaLocalTime(value);
        return Time.valueOf(localTime);
    }

    public static String toMySQLResultTimeText(long value) {
        Duration duration = Duration.ofMillis(value);
        LocalTime localTime = LocalTime.MIN.plus(Duration.ofMillis(value));
        int hour =localTime.getHour();
        int minute = localTime.getMinute();
        int second = localTime.getSecond();
        int millisOfSecond = duration.getNano();
        String v = localTime.toString();
        if(v.length()>TIME_STR_LEN_MAX_WITH_MICROS){
            System.out.println();
        }
        return v;
    }

}

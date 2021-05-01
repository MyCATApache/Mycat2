/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.resultset;

import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;

/**
 * @author Junwen Chen
 **/
public enum TextConvertorImpl implements TextConvertor {
    INSTANCE;

    @Override
    public byte[] convertBigDecimal(BigDecimal v) {
        return v.toPlainString().getBytes();
    }

    public static final byte[] ONE = "1".getBytes();
    public static final byte[] ZERO = "0".getBytes();

    @Override
    public byte[] convertBoolean(boolean v) {
        return v ? ONE : ZERO;
    }

    @Override
    public byte[] convertByte(byte v) {
        return String.valueOf(v).getBytes();
    }

    @Override
    public byte[] convertShort(short v) {
        return String.valueOf(v).getBytes();
    }

    @Override
    public byte[] convertInteger(int v) {
        return String.valueOf(v).getBytes();
    }

    @Override
    public byte[] convertLong(long v) {
        return String.valueOf(v).getBytes();
    }

    @Override
    public byte[] convertFloat(float v) {
        return String.valueOf(v).getBytes();
    }

    @Override
    public byte[] convertDouble(double v) {
        return String.valueOf(v).getBytes();
    }

    @Override
    public byte[] convertBytes(byte[] v) {
        return v;
    }

    @Override
    public byte[] convertDate(java.util.Date v) {
        return v.toString().getBytes();
    }

    @Override
    public byte[] convertTime(Time v) {
        return v.toString().getBytes();
    }

    @Override
    public byte[] convertTimeStamp(Timestamp v) {
        return v.toString().getBytes();
    }

    @Override
    public byte[] convertBlob(Blob v) {
        try {
            return v.getBytes(0, (int) v.length());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] convertClob(Clob v) {
        return v.toString().getBytes();
    }

    @Override
    public byte[] convertObject(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof byte[]) {
            return (byte[]) v;
        }
        return v.toString().getBytes();
    }

    @Override
    public byte[] convertDuration(Duration duration) {
        return getBytes(duration);
    }

    @NotNull
    public static byte[] getBytes(Duration duration) {
        return toString(duration).getBytes(StandardCharsets.UTF_8);
    }

    public static String toString(Duration duration) {
        long seconds = duration.getSeconds();
        int SECONDS_PER_HOUR = 60 * 60;
        int SECONDS_PER_MINUTE = 60;
        long hours = seconds / SECONDS_PER_HOUR;
        int minutes = (int) ((seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
        int secs = (int) (seconds % SECONDS_PER_MINUTE);
        int nano = duration.getNano();
        if (nano == 0) {
            return String.format("%02d:%02d:%02d", hours, minutes, secs);
        }
        return String.format("%02d:%02d:%02d.%09d", hours, minutes, secs, nano);
    }

    @Override
    public byte[] convertTime(LocalTime localTime) {
        return getBytes(localTime);
    }

    @NotNull
    public static byte[] getBytes(LocalTime localTime) {
        int hour = localTime.getHour();
        int minute = localTime.getMinute();
        int second = localTime.getSecond();
        int nano = localTime.getNano();
        if (nano == 0) {
            return String.format("%02d:%02d:%02d", hour, minute, second).getBytes();
        }
        return String.format("%02d:%02d:%02d.%09d", hour, minute, second, nano).getBytes();
    }

    @Override
    public byte[] convertTimeString(String s) {
        return s.getBytes();
    }

    @Override
    public byte[] convertTimeStamp(LocalDateTime value) {
        return getBytes(value);
    }

    @NotNull
    public static byte[] getBytes(LocalDateTime value) {
        int year = value.getYear();
        int monthValue = value.getMonthValue();
        int dayOfMonth = value.getDayOfMonth();
        int hour = value.getHour();
        int minute = value.getMinute();
        int second = value.getSecond();
        int nano = value.getNano()/1000;
        if (nano == 0) {
            return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    year,
                    monthValue,
                    dayOfMonth,
                    hour,
                    minute,
                    second
            ).getBytes();
        }
        return (String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",//"%04d-%02d-%02d %02d:%02d:%02d.%09d"
                year,
                monthValue,
                dayOfMonth,
                hour,
                minute,
                second,
                nano
        )).getBytes();
    }

    @Override
    public byte[] convertDate(LocalDate value) {
        return value.toString().getBytes();
    }
}
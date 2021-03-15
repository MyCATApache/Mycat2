/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.beans.mysql.packet;

import java.math.BigDecimal;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

/**
 * @author jamie12221
 *  date 2019-05-05 16:22
 *
 * 报文读视图
 **/
public interface MySQLPayloadReadView {

    int length();

    long readFixInt(int length);

    Long readLenencInt();

    String readFixString(int length);

    String readLenencString();

    byte[] readLenencStringBytes();

    byte[] readNULStringBytes();

    String readNULString();

    byte[] readEOFStringBytes();

    String readEOFString();

    byte[] readBytes(int length);

    byte[] readFixStringBytes(int length);

    byte readByte();

    byte[] readLenencBytes();

    long readLong();

    double readDouble();

    void reset();

    void skipInReading(int i);

    boolean readFinished();

    /**
     * time读取 binaryResultSet
     */
    default java.sql.Time readTime() {
        boolean negative;
        int days;
        int hours;
        int minutes;
        int seconds;
        int nanos;
        int length = readByte();
        if (length == 0) {
            negative = false;
            days = 0;
            hours = 0;
            minutes = 0;
            seconds = 0;
            nanos = 0;
        } else if (length == 8) {
            negative = readByte() == 1;
            days = (int) readFixInt(4);
            hours = readByte() & 0xff;

            minutes = readByte() & 0xff;
            seconds = readByte() & 0xff;
            nanos = 0;
        } else if (length == 12) {

            negative = readByte() == 1;
            days = (int) readFixInt(4);

            hours = readByte() & 0xff;
            minutes = readByte() & 0xff;
            seconds = readByte() & 0xff;
            byte[] bytes = readBytes(4);
            nanos = 1000 * (bytes[0] & 0xff) | ((bytes[1] & 0xff) << 8) | (
                    (bytes[2] & 0xff) << 16)
                    | ((bytes[3] & 0xff) << 24);
        } else {
            throw new RuntimeException("UNKOWN FORMAT");
        }
        if (negative) {
            days *= -1;
        }
        return Time.valueOf(LocalTime.of(days * 24 + hours, minutes, seconds, nanos));
    }

    /**
     * date 读取 BinaryResultSet
     */
    default java.util.Date readDate() {
        byte length = readByte();
        if (length == 0) {
            return java.sql.Date.valueOf(LocalDate.of(0, 0, 0));
        } else if (length == 4) {
            return java.sql.Date.valueOf(LocalDate.of((int) readFixInt(2), 0, 0));
        }
        int year = (int) readFixInt(2);
        int month = readByte() & 0xff;
        int date = readByte() & 0xff;
        int hour = readByte() & 0xff;
        int minute = readByte() & 0xff;
        int second = readByte() & 0xff;
        if (length == 7) {
            return java.sql.Timestamp.valueOf(LocalDateTime.of(year, month, date, hour, minute, second));
        }

        if (length == 11) {
            byte[] bytes = null;
            try {
                int nanos;
                bytes = readBytes(4);
                nanos = 1000 * (bytes[0] & 0xff) | ((bytes[1] & 0xff) << 8) | (
                        (bytes[2] & 0xff) << 16)
                        | ((bytes[3] & 0xff) << 24);
                return java.sql.Timestamp
                        .valueOf(LocalDateTime.of(year, month, date, hour, minute, second, nanos));
            }catch (Throwable throwable){
                System.out.println(Arrays.toString(bytes));
                return null;
            }
        } else {

            throw new RuntimeException("UNKOWN FORMAT");
        }
    }

    /**
     * Decimal 读取 BinaryResultSet
     */
    default BigDecimal readBigDecimal() {
        String src = readLenencString();
        return src == null ? null : new BigDecimal(src);
    }

    float readFloat();
}

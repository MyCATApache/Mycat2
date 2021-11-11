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
package io.mycat.beans.resultset;

import io.mycat.MySQLPacketUtil;
import io.mycat.util.ByteUtil;
import io.mycat.Datetimes;

public class SimpleTextWriterImpl implements ResultSetWriter {
    byte[][] row;
    short index = 0;

    @Override
    public void addFlagNull(boolean value) {
        row[index] = value ? null : row[index];

    }

    @Override
    public void startNewRow(int count) {
        this.row = new byte[count][];
    }

    @Override
    public void endNullMap() {
        index = 0;
    }


    @Override
    public void addBoolean(int i) {
        byte[] res;
        if (i > 0) {
            res = new byte[]{1};
        } else {
            res = new byte[]{0};
        }
        row[index] = res;
        index++;
    }

    @Override
    public void addInt8(byte b) {
        row[index] = String.valueOf(b).getBytes();
        index++;
    }

    @Override
    public void addInt16(short i) {
        row[index] =  String.valueOf(i).getBytes();
        index++;
    }

    @Override
    public void addChar(char i) {
        row[index] =  String.valueOf(i).getBytes();
        index++;
    }

    @Override
    public void addInt32(int i) {
        row[index] = String.valueOf(i).getBytes();
        index++;
    }

    @Override
    public void addInt64(long l) {
        row[index] =  String.valueOf(l).getBytes();
        index++;
    }

    @Override
    public void addFloat(float v) {
        row[index] = String.valueOf(v).getBytes();
        index++;
    }

    @Override
    public void addDouble(double v) {
        row[index] = String.valueOf(v).getBytes();
        index++;
    }

    @Override
    public void addString(byte[] bytes) {
        row[index] = bytes;
        index++;
    }

    @Override
    public void addBinary(byte[] bytes) {
        row[index] = bytes;
        index++;
    }

    @Override
    public void addUInt16(short c) {
        row[index] = String.valueOf(c).getBytes();
        index++;
    }

    @Override
    public void addUInt32(int i) {
        row[index] =String.valueOf(i).getBytes();
        index++;
    }

    @Override
    public void addUInt64(long l) {
        row[index] = String.valueOf(l).getBytes();
        index++;
    }

    @Override
    public void addDatetime(long l) {
        row[index] = Datetimes.toMySQLResultDatetimeText(l).getBytes();
        index++;
    }

    @Override
    public void addDate(long l) {
        row[index] = Datetimes.toMySQLResultDateText(l).getBytes();
        index++;
    }

    @Override
    public void addTime(int i) {
        row[index] = Datetimes.toMySQLResultTimeText(i).getBytes();
        index++;
    }

    @Override
    public void addUInt8(byte b) {
        row[index] =String.valueOf(b).getBytes();
        index++;
    }

    @Override
    public byte[] build() {
        return MySQLPacketUtil.generateTextRow(row);
    }
}

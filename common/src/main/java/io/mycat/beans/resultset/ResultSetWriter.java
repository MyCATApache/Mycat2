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

public interface ResultSetWriter {
    public void addFlagNull(boolean value);

    public void startNewRow(int value);

    public void addBoolean(boolean value);

    public void addInt8(byte value);

    public void addInt16(short value);

    public void addChar(char value);

    public void addInt32(int value);

    public void addInt64(long value);

    public void addFloat(float value);

    public void addDouble(double value);

    public void addString(byte[] value);

    public void addBinary(byte[] value);

    public void addUInt16(short value);

    public void addUInt32(int value);

    public void addUInt64(long value);

    public void addDatetime(long value);

    public void addDate(long value);

    public void addTime(int value);

    public void addUInt8(byte value);

    public byte[] build();
}

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

public class SimpleBinaryWriterImpl implements ResultSetWriter {
    Object[] objects;
    int index = 0;
    @Override
    public void addFlagNull(boolean value) {
        objects[index] = value?null:objects[index];
        index++;
    }

    @Override
    public void startNewRow(int value) {
        objects = new Object[value];
    }

    @Override
    public void addBoolean(boolean value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addInt8(byte value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addInt16(short value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addChar(char value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addInt32(int value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addInt64(long value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addFloat(float value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addDouble(double value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addString(byte[] value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addBinary(byte[] value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addUInt16(short value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addUInt32(int value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addUInt64(long value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addDatetime(long value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addDate(long value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addTime(int value) {
        objects[index] =value;
        index++;
    }

    @Override
    public void addUInt8(byte value) {
        objects[index] =value;
        index++;
    }

    @Override
    public byte[] build() {
       throw new UnsupportedOperationException();
    }
}

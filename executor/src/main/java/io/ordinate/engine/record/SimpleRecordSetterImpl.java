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

package io.ordinate.engine.record;

import io.ordinate.engine.function.BinarySequence;
import lombok.EqualsAndHashCode;
import org.jetbrains.annotations.NotNull;

@EqualsAndHashCode
public class SimpleRecordSetterImpl implements RecordSetter ,Comparable<SimpleRecordSetterImpl>{
   Object[] objects;
   byte index = 0;

    public SimpleRecordSetterImpl(int size) {
        this.objects = new Object[size];
    }

    public static RecordSetter create(int size){
       return new SimpleRecordSetterImpl(size);
   }
    @Override
    public void setNull() {
        objects[index++] = null;
    }

    @Override
    public void putBinary(BinarySequence value) {
        objects[index++] = value.getBytes();
    }

    @Override
    public void putBool(boolean value) {
        objects[index++] = value;
    }

    @Override
    public void putByte(byte value) {
        objects[index++] = value;
    }

    @Override
    public void putDate(long value) {
        objects[index++] = value;
    }

    @Override
    public void putDouble(double value) {
        objects[index++] = value;
    }

    @Override
    public void putFloat(float value) {
        objects[index++] = value;
    }

    @Override
    public void putInt(int value) {
        objects[index++] = value;
    }

    @Override
    public void putLong(long value) {
        objects[index++] = value;
    }

    @Override
    public void putShort(short value) {
        objects[index++] = value;
    }

    @Override
    public void putChar(char value) {
        objects[index++] = value;
    }

    @Override
    public void putString(CharSequence value) {
        objects[index++] = value;
    }

    @Override
    public void putDatetime(long value) {
        objects[index++] = value;
    }

    @Override
    public void putTime(long time) {
        objects[index++] = time;
    }

    @Override
    public void putUInt16(short uInt16) {
        objects[index++] = uInt16;
    }

    @Override
    public void putUInt64(long uInt64) {
        objects[index++] = uInt64;
    }

    @Override
    public void putSymbol(CharSequence symbol) {
        objects[index++] = symbol.toString();
    }

    @Override
    public void putObject(Object object) {
        objects[index++] = object;
    }

    @Override
    public void putUInt32(int uInt32) {
        objects[index++] = uInt32;
    }

    @Override
    public int compareTo(@NotNull SimpleRecordSetterImpl o) {

        return 0;
    }
}

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

import java.util.Arrays;
import java.util.Objects;

@EqualsAndHashCode
public class RecordImpl implements Record {
    final Object[] objects;
    boolean isNull;

    public static Record create(Object[] objects) {
        return new RecordImpl(objects);
    }

    public RecordImpl(Object[] objects) {
        this.objects = objects;
    }

    @Override
    public void setPosition(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Integer) object;
    }

    @Override
    public byte getByte(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Byte) object;
    }

    @Override
    public short getShort(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Short) object;
    }

    @Override
    public long getLong(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Long) object;
    }

    @Override
    public BinarySequence getBinary(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return null;
        return ((BinarySequence) object);
    }

    @Override
    public char getChar(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Character) object;
    }

    @Override
    public long getDatetime(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Long) object;
    }

    @Override
    public CharSequence getString(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return null;
        return (CharSequence) object;
    }

    @Override
    public float getFloat(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Float) object;
    }

    @Override
    public double getDouble(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Double) object;
    }

    @Override
    public long getTime(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Long) object;
    }

    @Override
    public boolean isNull(int columnIndex) {
        return isNull;
    }

    @Override
    public short getUInt16(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Short) object;
    }

    @Override
    public long getUInt64(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Long) object;
    }

    @Override
    public CharSequence getSymbol(int columnIndex) {
        return getString(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex) {
        return objects[columnIndex];
    }

    @Override
    public int getUInt32(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Integer) object;
    }

    @Override
    public long getDate(int columnIndex) {
        Object object = objects[columnIndex];
        if (isNull == (object == null)) return 0;
        return (Long) object;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordImpl record = (RecordImpl) o;
        return isNull == record.isNull && Arrays.equals(objects, record.objects);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(isNull);
        result = 31 * result + Arrays.hashCode(objects);
        return result;
    }
}

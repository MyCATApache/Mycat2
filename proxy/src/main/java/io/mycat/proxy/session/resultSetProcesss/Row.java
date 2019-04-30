/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.session.resultSetProcesss;

public interface Row<T extends Row<T>> {
    void setNotNullAt(int i);

    void setNullAt(int i);

    void setInt(int ordinal, int value);

    void setLong(int ordinal, long value);

    void setDouble(int ordinal, double value);

    void setBoolean(int ordinal, boolean value);

    void setShort(int ordinal, short value);

    void setByte(int ordinal, byte value);

    void setFloat(int ordinal, float value);

    boolean isNullAt(int ordinal);

    int getInt(int ordinal);

    long getLong(int ordinal);

    double getDouble(int ordinal);

    boolean getBoolean(int ordinal);

    short getShort(int ordinal);

    byte[] getBinary(int ordinal);

    byte getByte(int ordinal);

    float getFloat(int ordinal);

    T copy();

    public void pointTo(Object baseObject, long baseOffset, int sizeInBytes);

    default public void pointTo(Object baseObject, int sizeInBytes) {
        pointTo(baseObject, 0, sizeInBytes);
    }

    int numFields();

    public static int calculateBitSetWidthInBytes(int numFields) {
        return ((numFields + 63) / 64) * 8;
    }
}

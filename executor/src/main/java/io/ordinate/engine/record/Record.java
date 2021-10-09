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
import org.jetbrains.annotations.NotNull;

public interface Record extends Comparable<Record>{

    public void setPosition(int value);

    int getInt(int columnIndex);

    byte getByte(int columnIndex);

    short getShort(int columnIndex);

    long getLong(int columnIndex);

    BinarySequence getBinary(int columnIndex);

    char getChar(int columnIndex);

    long getDatetime(int columnIndex);

    CharSequence getString(int columnIndex);

    float getFloat(int columnIndex);

    double getDouble(int columnIndex);

    long getTime(int columnIndex);

    boolean isNull(int columnIndex);

    short getUInt16(int columnIndex);

    long getUInt64(int columnIndex);

    CharSequence getSymbol(int columnIndex);

    Object getObject(int columnIndex);

    int getUInt32(int i);

    long getDate(int columnIndex);

    default int getInt32Type(int columnIndex) {
        return getInt(columnIndex);
    }

    default int getUInt32Type(int columnIndex) {
        return getInt(columnIndex);
    }

    default boolean getBooleanType(int columnIndex) {
        return getInt(columnIndex)>0;
    }

    default byte getInt8Type(int columnIndex) {
        return getByte(columnIndex);
    }

    default byte getUInt8Type(int columnIndex) {
        return getByte(columnIndex);
    }

    default int getInt16Type(int columnIndex) {
        return getShort(columnIndex);
    }

    default int getUInt16Type(int columnIndex) {
        return getShort(columnIndex);
    }

    default Object getObjectType(int columnIndex) {
        return getObject(columnIndex);
    }

    default double getDoubleType(int columnIndex) {
        return getDouble(columnIndex);
    }

    default float getFloatType(int columnIndex) {
        return getFloat(columnIndex);
    }

    default CharSequence getStringType(int columnIndex) {
        return getString(columnIndex);
    }

    default CharSequence getSymbolType(int columnIndex) {
        return getSymbol(columnIndex);
    }

    default long getTimeMilliType(int columnIndex) {
        return getTime(columnIndex);
    }

    default long getDatetimeMilliType(int columnIndex) {
        return getDatetime(columnIndex);
    }

    default char getCharType(int columnIndex) {
        return getChar(columnIndex);
    }

    default long getDateType(int columnIndex) {
        return getDate(columnIndex);
    }

    default BinarySequence getBinaryType(int columnIndex) {
        return getBinary(columnIndex);
    }

    default long getInt64Type(int columnIndex) {
        return getLong(columnIndex);
    }

    default long getUInt64Type(int columnIndex) {
        return getLong(columnIndex);
    }

    @Override
   default int compareTo(@NotNull Record o){
        throw new UnsupportedOperationException();
    }
}

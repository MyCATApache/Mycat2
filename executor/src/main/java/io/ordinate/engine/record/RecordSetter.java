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

public interface RecordSetter {
    void setNull();
    void putBinary(BinarySequence value);

    void putBool(boolean value);

    void putByte(byte value);

    void putDate(long value);

    void putDouble(double value);

    void putFloat(float value);

    void putInt(int value);

    void putLong(long value);

    void putShort(short value);

    void putChar(char value);

    void putString(CharSequence value);

    void putDatetime(long value);

    void putTime(long time);

    void putUInt16(short uInt16);

    void putUInt64(long uInt64);

    void putSymbol(CharSequence symbol);

    void putObject(Object object);

    void putUInt32(int uInt32);
}

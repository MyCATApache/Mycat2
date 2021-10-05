/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/
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

package io.ordinate.engine.function.constant;

import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.function.ScalarFunction;

public final class NullConstant implements ConstantFunction, ScalarFunction {

    public static final NullConstant NULL = new NullConstant();
    final InnerType type;


    private NullConstant() {
        this.type = InnerType.NULL_TYPE;
    }

    private NullConstant(InnerType type) {
        this.type = type;
    }

    public static NullConstant create(InnerType type) {
        return new NullConstant(type);
    }

    @Override
    public InnerType getType() {
        return InnerType.NULL_TYPE;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }


    @Override
    public BinarySequence getBinary(Record rec) {
        return NullBinaryConstant.INSTANCE.getBinary(null);
    }

    @Override
    public byte getByte(Record rec) {
        return ByteConstant.ZERO.getByte(null);
    }

    @Override
    public char getChar(Record rec) {
        return CharConstant.ZERO.getChar(null);
    }

    @Override
    public double getDouble(Record rec) {
        return DoubleConstant.NULL.getDouble(null);
    }

    @Override
    public float getFloat(Record rec) {
        return FloatConstant.NULL.getFloat(null);
    }

    @Override
    public short getShort(Record rec) {
        return ShortConstant.ZERO.getShort(null);
    }

    @Override
    public int getInt(Record rec) {
        return IntConstant.NULL.getInt(null);
    }

    @Override
    public long getLong(Record rec) {
        return LongConstant.NULL.getLong(null);
    }

    @Override
    public CharSequence getString(Record rec) {
        return StringConstant.NULL.getString(null);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return SymbolConstant.NULL.getSymbol(null);
    }

    @Override
    public long getDatetime(Record rec) {
        return DatetimeConstant.NULL.getDatetime(null);
    }

    @Override
    public long getDate(Record rec) {
        return DateConstant.NULL.getDate(null);
    }

}

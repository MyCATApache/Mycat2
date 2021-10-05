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

package io.ordinate.engine.function.bind;

import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.function.ScalarFunction;
import io.ordinate.engine.function.Function;
import lombok.Getter;

import java.util.Collections;
import java.util.List;

@Getter
public class IndexedParameterLinkFunction implements ScalarFunction {
    private final int variableIndex;
    private Function base;

    public IndexedParameterLinkFunction(int variableIndex, Function base) {
        this.variableIndex = variableIndex;
        this.base = base;
    }

    @Override
    public List<Function> getArgs() {
        return Collections.emptyList();
    }


    @Override
    public InnerType getType() {
        return this.base.getType();
    }

    @Override
    public BinarySequence getBinary(Record rec) {
        return base.getBinary(rec);
    }

    @Override
    public byte getByte(Record rec) {
        return base.getByte(rec);
    }

    @Override
    public char getChar(Record rec) {
        return base.getChar(rec);
    }

    @Override
    public long getDate(Record rec) {
        return base.getDate(rec);
    }

    @Override
    public double getDouble(Record rec) {
        return base.getDouble(rec);
    }

    @Override
    public float getFloat(Record rec) {
        return base.getFloat(rec);
    }

    @Override
    public int getInt(Record rec) {
        return base.getInt(rec);
    }

    @Override
    public long getLong(Record rec) {
        return base.getLong(rec);
    }

    @Override
    public short getShort(Record rec) {
        return base.getShort(rec);
    }

    @Override
    public CharSequence getString(Record rec) {
        return base.getString(rec);
    }

    @Override
    public long getDatetime(Record rec) {
        return base.getDatetime(rec);
    }

    @Override
    public long getTime(Record rec) {
        return base.getTime(rec);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return base.getSymbol(rec);
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }
}

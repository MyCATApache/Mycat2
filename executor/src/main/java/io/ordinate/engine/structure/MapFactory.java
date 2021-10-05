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
package io.ordinate.engine.structure;

import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.schema.IntInnerType;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.FastMap;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class MapFactory {
    public static Map createMap(InnerType... pairs){
        IntInnerType[] intPairs = getIntPairs(pairs);
        return createMap(intPairs);
    }

    @NotNull
    public static Map createMap(IntInnerType[] intPairs) {
        return createMap(intPairs,new InnerType[]{});
    }

    @NotNull
    private static IntInnerType[] getIntPairs(InnerType[] pairs) {
        int index = 0;
        IntInnerType[] intPairs = new IntInnerType[pairs.length];
        for (InnerType pair : pairs) {
            intPairs[index] = IntInnerType.of(index,pair);
            index++;
        }
        return intPairs;
    }

    public static Map createMap(IntInnerType[] keys, InnerType[] values){
        ColumnTypes keyTypes = getKeyTypes(keys);
        int[] valueTypes = Arrays.stream(values).mapToInt(i -> toQuestDbType(i)).toArray();
        int MB = 1024 * 1024;
        return new FastMap(
                16 * MB,
                keyTypes
                , new ColumnTypes() {
            @Override
            public int getColumnCount() {
                return valueTypes.length;
            }

            @Override
            public int getColumnType(int columnIndex) {
                return valueTypes[columnIndex];
            }
        },
                128,
                0.5,
                64);
    }
    public static Map createMap2(InnerType[] pairs,ColumnTypes valueTypes ){
       IntInnerType[] intPairs = getIntPairs(pairs);
        int MB = 1024 * 1024;
        return new FastMap(
                16 * MB,
            getKeyTypes(intPairs)
                , valueTypes,
                128,
                0.5,
                64);
    }
    @NotNull
    private static ColumnTypes getKeyTypes(IntInnerType[] keys) {
        return new ColumnTypes() {
            @Override
            public int getColumnCount() {
                return keys.length;
            }

            @Override
            public int getColumnType(int columnIndex) {
                InnerType type = keys[columnIndex].type;
                return toQuestDbType(type);
            }
        };
    }

    private static int toQuestDbType(InnerType type) {
        switch (type) {
            case BOOLEAN_TYPE:
                return ColumnType.BOOLEAN;
            case UINT8_TYPE:
            case INT8_TYPE:
                return ColumnType.BYTE;
            case UINT16_TYPE:
            case INT16_TYPE:
                return ColumnType.SHORT;
            case CHAR_TYPE:
                return ColumnType.CHAR;
            case UINT32_TYPE:
            case INT32_TYPE:
                return ColumnType.INT;
            case UINT64_TYPE:
            case INT64_TYPE:
                return ColumnType.LONG;
            case FLOAT_TYPE:
                return ColumnType.FLOAT;
            case DOUBLE_TYPE:
                return ColumnType.DOUBLE;
            case STRING_TYPE:
                return ColumnType.STRING;
            case BINARY_TYPE:
                return ColumnType.BINARY;
            case TIME_MILLI_TYPE:
            case DATE_TYPE:
            case DATETIME_MILLI_TYPE:
                return ColumnType.LONG;
            case SYMBOL_TYPE:
            case OBJECT_TYPE:
            case NULL_TYPE:
                return ColumnType.STRING;

            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }


    public static void main(String[] args) {
        Map map = MapFactory.createMap(InnerType.STRING_TYPE);
        MapKey mapKey = map.withKey();
        MapValue value = mapKey.findValue();
        boolean b = mapKey.notFound();
        mapKey.putStr("aaa");
        MapValue value1 = mapKey.createValue();

        MapValue value2 = mapKey.findValue();
        System.out
                .println();
        map.close();
    }
}

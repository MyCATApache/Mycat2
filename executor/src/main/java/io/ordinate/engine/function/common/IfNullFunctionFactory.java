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

package io.ordinate.engine.function.common;

import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.schema.InnerType;

import java.util.List;

public class IfNullFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "ifNull(object,object)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Function() {
            Function left;
            Function right;
            boolean isNull;

            @Override
            public InnerType getType() {
                return left.getType();
            }

            @Override
            public List<Function> getArgs() {
                return args;
            }

            @Override
            public BinarySequence getBinary(Record rec) {
                isNull = false;
                BinarySequence binary = left.getBinary(rec);
                if(!left.isNull(rec)){
                    return binary;
                }
                 binary = right.getBinary(rec);
                if(!right.isNull(rec)){
                    return binary;
                }
                isNull = true;
                return null;
            }

            @Override
            public byte getByte(Record rec) {
                isNull = false;
                byte aByte = left.getByte(rec);
                if(!left.isNull(rec)){
                    return aByte;
                }
                aByte = right.getByte(rec);
                if(!right.isNull(rec)){
                    return aByte;
                }
                isNull = true;
                return 0;
            }

            @Override
            public char getChar(Record rec) {
                isNull = false;
                char aChar = left.getChar(rec);
                if(!left.isNull(rec)){
                    return aChar;
                }
                aChar = right.getChar(rec);
                if(!right.isNull(rec)){
                    return aChar;
                }
                isNull = true;
                return 0;
            }

            @Override
            public long getDate(Record rec) {
                isNull = false;
                long date = left.getDate(rec);
                if(!left.isNull(rec)){
                    return date;
                }
                date = right.getDate(rec);
                if(!right.isNull(rec)){
                    return date;
                }
                isNull = true;
                return 0;
            }

            @Override
            public double getDouble(Record rec) {
                isNull = false;
                double aDouble = left.getDouble(rec);
                if(!left.isNull(rec)){
                    return aDouble;
                }
                aDouble = right.getDouble(rec);
                if(!right.isNull(rec)){
                    return aDouble;
                }
                isNull = true;
                return 0;
            }

            @Override
            public float getFloat(Record rec) {
                isNull = false;
                float aFloat = left.getFloat(rec);
                if(!left.isNull(rec)){
                    return aFloat;
                }
                aFloat = right.getFloat(rec);
                if(!right.isNull(rec)){
                    return aFloat;
                }
                isNull = true;
                return 0;
            }

            @Override
            public int getInt(Record rec) {
                isNull = false;
                int anInt = left.getInt(rec);
                if(!left.isNull(rec)){
                    return anInt;
                }
                anInt = right.getInt(rec);
                if(!right.isNull(rec)){
                    return anInt;
                }
                isNull = true;
                return 0;
            }

            @Override
            public long getLong(Record rec) {
                isNull = false;
                long aLong = left.getLong(rec);
                if(!left.isNull(rec)){
                    return aLong;
                }
                aLong = right.getLong(rec);
                if(!right.isNull(rec)){
                    return aLong;
                }
                isNull = true;
                return 0;
            }

            @Override
            public short getShort(Record rec) {
                isNull = false;
                short aLong = left.getShort(rec);
                if(!left.isNull(rec)){
                    return aLong;
                }
                aLong = right.getShort(rec);
                if(!right.isNull(rec)){
                    return aLong;
                }
                isNull = true;
                return 0;
            }

            @Override
            public CharSequence getString(Record rec) {
                isNull = false;
                CharSequence charSequence = left.getString(rec);
                if(!left.isNull(rec)){
                    return charSequence;
                }
                charSequence = right.getString(rec);
                if(!right.isNull(rec)){
                    return charSequence;
                }
                isNull = true;
                return null;
            }

            @Override
            public long getDatetime(Record rec) {
                isNull = false;
                long datetime = left.getDatetime(rec);
                if(!left.isNull(rec)){
                    return datetime;
                }
                datetime = right.getDatetime(rec);
                if(!right.isNull(rec)){
                    return datetime;
                }
                isNull = true;
                return 0;
            }

            @Override
            public long getTime(Record rec) {
                isNull = false;
                long time = left.getTime(rec);
                if(!left.isNull(rec)){
                    return time;
                }
                time = right.getTime(rec);
                if(!right.isNull(rec)){
                    return time;
                }
                isNull = true;
                return 0;
            }

            @Override
            public CharSequence getSymbol(Record rec) {
                isNull = false;
                CharSequence symbol = left.getSymbol(rec);
                if(!left.isNull(rec)){
                    return symbol;
                }
                symbol = right.getSymbol(rec);
                if(!right.isNull(rec)){
                    return symbol;
                }
                isNull = true;
                return null;
            }

            @Override
            public boolean isNull(Record rec) {
                return isNull;
            }
        };
    }
}

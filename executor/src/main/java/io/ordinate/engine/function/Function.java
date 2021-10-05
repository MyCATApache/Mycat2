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

package io.ordinate.engine.function;


import io.ordinate.engine.record.Record;
import io.ordinate.engine.record.VectorBatchRecord;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.vector.VectorContext;

import java.util.Collections;
import java.util.List;

public interface Function {

    default List<Function> getArgs() {
        return Collections.emptyList();
    }

    default void visit(FunctionVisitor visitor) {
        for (Function arg : getArgs()) {
            arg.visit(visitor);
        }
        visitor.accept(this);
    }


    default boolean getBooleanType(Record rec) {
        return getInt(rec)>0;
    }


    default byte getInt8Type(Record rec) {
        return getByte(rec);
    }

    default short getInt16Type(Record rec) {
        return getShort(rec);
    }

    default char getCharType(Record rec) {
        return getChar(rec);
    }

    default int getInt32Type(Record rec) {
        return getInt(rec);
    }

    default long getInt64Type(Record rec) {
        return getLong(rec);
    }

    default float getFloatType(Record rec) {
        return getFloat(rec);
    }

    default double getDoubleType(Record rec) {
        return getDouble(rec);
    }

    default CharSequence getStringType(Record rec) {
        return getString(rec);
    }

    default BinarySequence getBinaryType(Record rec) {
        return getBinary(rec);
    }

    default byte getUInt8Type(Record rec) {
        return getByte(rec);
    }

    default short getUInt16Type(Record rec) {
        return getShort(rec);
    }

    default int getUInt32Type(Record rec) {
        return getInt(rec);
    }

    default long getUInt64Type(Record rec) {
        return getLong(rec);
    }

    default long getTimeMilliType(Record rec) {
        return getTime(rec);
    }

    default long getDateType(Record rec) {
        return getDate(rec);
    }

    default long getDatetimeMilliType(Record rec) {
        return getDatetime(rec);
    }

    default CharSequence getSymbolType(Record rec) {
        return getSymbol(rec);
    }

    default byte getByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    default char getChar(Record rec) {
        throw new UnsupportedOperationException();
    }

    default long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    default double getDouble(Record rec) {
        throw new UnsupportedOperationException();
    }

    default float getFloat(Record rec) {
        throw new UnsupportedOperationException();
    }

    default int getInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    default long getLong(Record rec) {
        throw new UnsupportedOperationException();
    }


    default short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    default CharSequence getString(Record rec) {
        throw new UnsupportedOperationException();
    }

    default long getDatetime(Record rec) {
        throw new UnsupportedOperationException();
    }

    default long getTime(Record rec) {
        throw new UnsupportedOperationException();
    }

    default CharSequence getSymbol(Record rec) {
        throw new UnsupportedOperationException();
    }

    default BinarySequence getBinary(Record rec) {
        throw new UnsupportedOperationException();
    }

    InnerType getType();

    default boolean isConstant() {
        return false;
    }

    default boolean isNullableConstant() {
        return true;
    }

    default boolean isNull(Record rec) {
        return false;
    }

    default boolean isRuntimeConstant() {
        return false;
    }


    default public void close() {

    }

    default Record generateSink(VectorContext vContext) {
        return new VectorBatchRecord(vContext.getVectorSchemaRoot());
    }

}

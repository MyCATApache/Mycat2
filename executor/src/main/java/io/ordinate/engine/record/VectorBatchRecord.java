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
import org.apache.arrow.vector.*;

public class VectorBatchRecord implements Record {
    VectorSchemaRoot vectorSchemaRoot;
    int index = 0;
    boolean isNull;
    public VectorBatchRecord(VectorSchemaRoot vectorSchemaRoot) {
        this.vectorSchemaRoot = vectorSchemaRoot;
    }

    public void setPosition(int value){
        this.index = value;
    }

    @Override
    public int getInt(int columnIndex) {
        return ((IntVector) this.vectorSchemaRoot.getVector(columnIndex)).get(index);
    }

    @Override
    public long getLong(int columnIndex) {
        FieldVector vector = this.vectorSchemaRoot.getVector(columnIndex);
        BaseFixedWidthVector nullVector = (BaseFixedWidthVector)vector;
        BaseIntVector valueVector = (BaseIntVector)vector;
        if (isNull = nullVector.isNull(index))return 0;
        return valueVector.getValueAsLong(index);
    }

    @Override
    public byte getByte(int columnIndex) {
        return ((TinyIntVector)this.vectorSchemaRoot.getVector(columnIndex)).get(index);
    }

    @Override
    public short getShort(int columnIndex) {
        return 0;
    }

    @Override
    public BinarySequence getBinary(int columnIndex) {
        return null;
    }

    @Override
    public char getChar(int columnIndex) {
        return 0;
    }

    @Override
    public long getDate(int columnIndex) {
        return 0;
    }

    @Override
    public long getDatetime(int columnIndex) {
        return 0;
    }

    @Override
    public CharSequence getString(int columnIndex) {
        return null;
    }

    @Override
    public CharSequence getSymbol(int columnIndex) {
        return null;
    }

    @Override
    public float getFloat(int columnIndex) {
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) {
        FieldVector vector = this.vectorSchemaRoot.getVector(columnIndex);
        BaseFixedWidthVector nullVector = (BaseFixedWidthVector)vector;
        FloatingPointVector valueVector = (FloatingPointVector)vector;
        if (isNull = nullVector.isNull(index))return 0;
        return valueVector.getValueAsDouble(index);
    }

    @Override
    public long getTime(int columnIndex) {
        return 0;
    }

    @Override
    public boolean isNull(int columnIndex) {
        if (isNull)return true;
        return   this.vectorSchemaRoot.getVector(columnIndex).isNull(index);
    }

    @Override
    public short getUInt16(int columnIndex) {
        return 0;
    }

    @Override
    public long getUInt64(int columnIndex) {
        return 0;
    }

    @Override
    public Object getObject(int columnIndex) {
        return null;
    }

    @Override
    public int getUInt32(int i) {
        return 0;
    }
}

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
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class JoinRecord implements Record {
    Record left;
    Record right;
    int leftColumnCount;

    public static JoinRecord create(Record left, Record right, int leftColumnCount){
        return new JoinRecord(left,right,leftColumnCount);
    }

    public JoinRecord(Record left, Record right, int leftColumnCount) {
        this.left = left;
        this.right = right;
        this.leftColumnCount = leftColumnCount;
    }

    @Override
    public void setPosition(int value) {
        left.setPosition(value);
        right.setPosition(value);
    }

    @Override
    public int getInt(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getInt(columnIndex);
        }
        return right.getInt(columnIndex - leftColumnCount);
    }

    @Override
    public byte getByte(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getByte(columnIndex);
        }
        return right.getByte(columnIndex - leftColumnCount);
    }

    @Override
    public short getShort(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getShort(columnIndex);
        }
        return right.getShort(columnIndex - leftColumnCount);
    }

    @Override
    public long getLong(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getLong(columnIndex);
        }
        return right.getLong(columnIndex - leftColumnCount);
    }

    @Override
    public BinarySequence getBinary(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getBinary(columnIndex);
        }
        return right.getBinary(columnIndex - leftColumnCount);
    }

    @Override
    public char getChar(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getChar(columnIndex);
        }
        return right.getChar(columnIndex - leftColumnCount);
    }

    @Override
    public long getDatetime(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getDatetime(columnIndex);
        }
        return right.getDatetime(columnIndex - leftColumnCount);
    }

    @Override
    public CharSequence getString(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getString(columnIndex);
        }
        return right.getString(columnIndex - leftColumnCount);
    }

    @Override
    public float getFloat(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getFloat(columnIndex);
        }
        return right.getFloat(columnIndex - leftColumnCount);
    }

    @Override
    public double getDouble(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getDouble(columnIndex);
        }
        return right.getDouble(columnIndex - leftColumnCount);
    }

    @Override
    public long getTime(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getTime(columnIndex);
        }
        return right.getTime(columnIndex - leftColumnCount);
    }

    @Override
    public boolean isNull(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.isNull(columnIndex);
        }
        return right.isNull(columnIndex - leftColumnCount);
    }

    @Override
    public short getUInt16(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getUInt16(columnIndex);
        }
        return right.getUInt16(columnIndex - leftColumnCount);
    }

    @Override
    public long getUInt64(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getUInt64(columnIndex);
        }
        return right.getUInt64(columnIndex - leftColumnCount);
    }

    @Override
    public CharSequence getSymbol(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getSymbol(columnIndex);
        }
        return right.getSymbol(columnIndex - leftColumnCount);
    }

    @Override
    public Object getObject(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getObject(columnIndex);
        }
        return right.getObject(columnIndex - leftColumnCount);
    }

    @Override
    public int getUInt32(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getUInt32(columnIndex);
        }
        return right.getUInt32(columnIndex - leftColumnCount);
    }

    @Override
    public long getDate(int columnIndex) {
        if (columnIndex < leftColumnCount) {
            return left.getDate(columnIndex);
        }
        return right.getDate(columnIndex - leftColumnCount);
    }
}

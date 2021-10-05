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

package io.ordinate.engine.builder;

import com.google.common.primitives.UnsignedLong;
import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.physicalplan.SortPlan;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.schema.InnerType;
import lombok.Getter;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Comparator;
import java.util.List;

@Getter
public class PhysicalSortProperty {
    SortOptions sortOptions;
    int columnIndex;
    InnerType type;

    public static PhysicalSortProperty of(int columnIndex, SortOptions sortOptions, InnerType type) {
        return new PhysicalSortProperty(sortOptions, columnIndex, type);
    }

    public PhysicalSortProperty(SortOptions sortOptions, int columnIndex, InnerType type) {
        this.sortOptions = sortOptions;
        this.columnIndex = columnIndex;
        this.type = type;
    }

    public SortPlan.SortColumn evaluateToSortColumn(VectorSchemaRoot input) {
        return new SortPlan.SortColumn(input.getVector(columnIndex), sortOptions);
    }

    public Comparator<Record> evaluateToSortComparator() {
        SortOptions options = this.sortOptions;

        Comparator<Record> comparator = new Comparator<Record>() {
            @Override
            public int compare(Record o1, Record o2) {
                boolean isNull1 = o1.isNull(columnIndex);
                boolean isNull2 = o2.isNull(columnIndex);

                if (isNull1 || isNull2) {
                    if (isNull1 && isNull2) {
                        return 0;
                    } else if (isNull1) {
                        if (options.nullsFirst) {
                            return -1;       // null1 is smaller
                        } else {
                            return 1;
                        }
                    } else {
                        if (options.nullsFirst) {
                            return 1;       // null2 is smaller
                        } else {
                            return -1;
                        }
                    }
                }
                return compareNotNull(o1, o2);
            }

            private int compareNotNull(Record left, Record right) {
                switch (type) {
                    case BOOLEAN_TYPE: {
                        return Boolean.compare(left.getBooleanType(columnIndex), right.getBooleanType(columnIndex));
                    }
                    case INT8_TYPE: {
                        return Integer.compare(left.getInt(columnIndex), right.getInt(columnIndex));
                    }
                    case INT16_TYPE: {
                        return Short.compare(left.getShort(columnIndex), right.getShort(columnIndex));
                    }
                    case CHAR_TYPE: {
                        return Character.compare(left.getCharType(columnIndex), right.getChar(columnIndex));
                    }
                    case INT32_TYPE: {
                        return Integer.compare(left.getInt32Type(columnIndex), right.getInt32Type(columnIndex));
                    }
                    case INT64_TYPE: {
                        return Long.compare(left.getInt64Type(columnIndex), right.getInt64Type(columnIndex));
                    }
                    case FLOAT_TYPE: {
                        return Float.compare(left.getFloatType(columnIndex), right.getFloatType(columnIndex));
                    }
                    case DOUBLE_TYPE: {
                        return Double.compare(left.getDoubleType(columnIndex), right.getDoubleType(columnIndex));
                    }
                    case STRING_TYPE: {
                        return left.getString(columnIndex).toString().compareTo(right.getString(columnIndex).toString());
                    }
                    case BINARY_TYPE: {
                        BinarySequence leftBinary = left.getBinary(columnIndex);
                        BinarySequence rightBinary = right.getBinary(columnIndex);
                        return leftBinary.compareTo(rightBinary);
                    }
                    case UINT8_TYPE: {
                        int l = Byte.toUnsignedInt(left.getUInt8Type(columnIndex));
                        int r = Byte.toUnsignedInt(right.getUInt8Type(columnIndex));
                        return Integer.compare(l, r);
                    }
                    case UINT16_TYPE: {
                        int l = Short.toUnsignedInt(left.getUInt16(columnIndex));
                        int r = Short.toUnsignedInt(right.getUInt16(columnIndex));
                        return Integer.compare(l, r);
                    }
                    case UINT32_TYPE: {
                        long l = Integer.toUnsignedLong(left.getUInt32(columnIndex));
                        long r = Integer.toUnsignedLong(right.getUInt32(columnIndex));
                        return Long.compare(l, r);
                    }
                    case UINT64_TYPE: {
                        return UnsignedLong.fromLongBits(left.getUInt64(columnIndex)).compareTo(UnsignedLong.fromLongBits(right.getUInt64(columnIndex)));
                    }
                    case TIME_MILLI_TYPE: {
                        return UnsignedLong.fromLongBits(left.getTime(columnIndex)).compareTo(UnsignedLong.fromLongBits(right.getTime(columnIndex)));
                    }
                    case DATE_TYPE: {
                        return UnsignedLong.fromLongBits(left.getDate(columnIndex)).compareTo(UnsignedLong.fromLongBits(right.getDate(columnIndex)));
                    }
                    case DATETIME_MILLI_TYPE: {
                        return UnsignedLong.fromLongBits(left.getDatetime(columnIndex)).compareTo(UnsignedLong.fromLongBits(right.getDatetime(columnIndex)));
                    }
                    case SYMBOL_TYPE: {
                        return left.getSymbol(columnIndex).toString().compareTo((right.getSymbol(columnIndex)).toString());
                    }
                    case OBJECT_TYPE:
                    case NULL_TYPE:
                    default:
                        throw new IllegalArgumentException();
                }
            }
        };
        if (options.descending) {
            comparator = comparator.reversed();
        }
        return comparator;
    }


    public   static Comparator<Record> getRecordComparator(List<PhysicalSortProperty> physicalSortProperties) {
        Comparator<Record> recordComparator;
        if (physicalSortProperties.size()==1){
            recordComparator = physicalSortProperties.get(0).evaluateToSortComparator();
        }else {
            recordComparator = physicalSortProperties.get(0).evaluateToSortComparator();
            for (PhysicalSortProperty physicalSortProperty : physicalSortProperties.subList(1, physicalSortProperties.size())) {
                recordComparator=   recordComparator.thenComparing( physicalSortProperty.evaluateToSortComparator());
            }
        }
        return recordComparator;
    }

}

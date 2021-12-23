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

package io.ordinate.engine.schema;


import io.ordinate.engine.function.BinarySequence;
import lombok.Getter;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.sql.JDBCType;
import java.util.List;

@Getter
public enum InnerType {
    BOOLEAN_TYPE(ArrowTypes.BOOLEAN_TYPE, BitVector.class, boolean.class, "bool", false, JDBCType.BOOLEAN),
    INT8_TYPE(ArrowTypes.INT8_TYPE, TinyIntVector.class, byte.class, "int8", true, JDBCType.INTEGER),
    INT16_TYPE(ArrowTypes.INT16_TYPE, SmallIntVector.class, short.class, "int16", true, JDBCType.SMALLINT),
    CHAR_TYPE(ArrowTypes.UINT16_TYPE, UInt2Vector.class, char.class, "char", false, JDBCType.CHAR),
    INT32_TYPE(ArrowTypes.INT32_TYPE, IntVector.class, int.class, "int32", true, JDBCType.INTEGER),
    INT64_TYPE(ArrowTypes.INT64_TYPE, BigIntVector.class, long.class, "int64", true, JDBCType.BIGINT),
    FLOAT_TYPE(ArrowTypes.FLOAT_TYPE, Float4Vector.class, float.class, "float", false, JDBCType.FLOAT),
    DOUBLE_TYPE(ArrowTypes.DOUBLE_TYPE, Float8Vector.class, double.class, "double", false, JDBCType.DOUBLE),
    DECIMAL_TYPE(ArrowTypes.DECIMAL_TYPE, DecimalVector.class, float.class, "decimal", false, JDBCType.DECIMAL),
    STRING_TYPE(ArrowTypes.STRING_TYPE, VarCharVector.class, String.class, "string", false, JDBCType.VARCHAR),
    BINARY_TYPE(ArrowTypes.BINARY_TYPE, VarBinaryVector.class, BinarySequence.class, "binary", false, JDBCType.BINARY),


    UINT8_TYPE(ArrowTypes.INT8_TYPE, UInt1Vector.class, byte.class, "uint8", false, JDBCType.TINYINT),
    UINT16_TYPE(ArrowTypes.INT16_TYPE, UInt2Vector.class, short.class, "uint16", false, JDBCType.SMALLINT),
    UINT32_TYPE(ArrowTypes.INT32_TYPE, UInt4Vector.class, int.class, "uint32", false, JDBCType.INTEGER),
    UINT64_TYPE(ArrowTypes.INT64_TYPE, UInt8Vector.class, long.class, "uint64", false, JDBCType.INTEGER),

    TIME_MILLI_TYPE(ArrowTypes.TIME_MILLI_TYPE, TimeMilliVector.class, long.class, "time", false, JDBCType.TIME),
    DATE_TYPE(ArrowTypes.DATE_TYPE, DateMilliVector.class, int.class, "date", false, JDBCType.DATE),
    DATETIME_MILLI_TYPE(ArrowTypes.DATETIME_MILLI_TYPE, TimeStampMilliVector.class, long.class, "datetime", false, JDBCType.TIMESTAMP),
    SYMBOL_TYPE(null, null, String.class, "symbol", false, JDBCType.VARCHAR),
    OBJECT_TYPE(null, null, Object.class, "object", false, JDBCType.JAVA_OBJECT),
    NULL_TYPE(null, null, Void.class, "null", false, JDBCType.NULL),
    ;
    private ArrowType arrowType;
    Class<? extends FieldVector> fieldVector;
    private Class javaClass;
    private String alias;
    private boolean signed;
    private JDBCType jdbcType;

    InnerType(ArrowType arrowType, Class<? extends FieldVector> fieldVector, Class javaClass, String name, boolean signed, JDBCType jdbcType) {
        this.arrowType = arrowType;
        this.fieldVector = fieldVector;
        this.javaClass = javaClass;
        this.alias = name;
        this.signed = signed;
        this.jdbcType = jdbcType;
    }

    public static ArrowType toArrow(InnerType innerType) {
        return innerType.arrowType;
    }

    public static InnerType from(ArrowType type) {
        for (InnerType value : values()) {
            if (value.arrowType.equals(type)) {
                return value;

            }
        }
        throw new IllegalArgumentException();
    }

    public static boolean canCast(FieldVector sourceVector, FieldVector targetVector) {
        ArrowType.ArrowTypeID sourceTypeId = sourceVector.getMinorType().getType().getTypeID();
        ArrowType.ArrowTypeID targetTypeId = targetVector.getMinorType().getType().getTypeID();
        switch (sourceTypeId) {
            case Null: {
                return true;
            }
            case Struct:
            case List:
            case LargeList:
            case FixedSizeList:
            case Union:
            case Map: {
                return false;
            }
            case Int:
            case FloatingPoint:
            case Decimal:
            case Bool: {
                switch (targetTypeId) {
                    case Null:
                    case Decimal:
                    case Bool:
                    case Int:
                    case FloatingPoint:
                    case Utf8:
                    case LargeUtf8:
                        return true;
                    case Struct:
                    case List:
                    case LargeList:
                    case FixedSizeList:
                    case Union:
                    case Map:
                    case Interval:
                    case Timestamp:
                    case Time:
                    case Date:
                    case Binary:
                    case LargeBinary:
                    case FixedSizeBinary:
                    case Duration:
                    case NONE:
                        return false;
                }
                return false;
            }
            case Utf8:
            case LargeUtf8:
                return true;
            case Binary:
            case LargeBinary:
            case FixedSizeBinary:
                return false;
            case Date: {
                switch (targetTypeId) {
                    case Null:
                    case Decimal:
                    case Bool:
                    case Int:
                    case FloatingPoint:
                    case Utf8:
                    case LargeUtf8:
                    case Struct:
                    case List:
                    case LargeList:
                    case FixedSizeList:
                    case Union:
                    case Map:
                    case Interval:
                    case Binary:
                    case LargeBinary:
                    case FixedSizeBinary:
                    case Duration:
                    case NONE:
                    case Time:
                        return false;
                    case Timestamp:
                    case Date:
                        return true;
                }
                return false;
            }
            case Time: {
                switch (targetTypeId) {
                    case Null:
                    case Decimal:
                    case Bool:
                    case Int:
                    case FloatingPoint:
                    case Utf8:
                    case LargeUtf8:
                    case Struct:
                    case List:
                    case LargeList:
                    case FixedSizeList:
                    case Union:
                    case Map:
                    case Interval:
                    case Binary:
                    case LargeBinary:
                    case FixedSizeBinary:
                    case Duration:
                    case Timestamp:
                    case Date:
                    case NONE:
                        return false;
                    case Time:
                        return true;
                }
                return false;
            }
            case Timestamp: {
                switch (targetTypeId) {
                    case Null:
                    case Decimal:
                    case Bool:
                    case Int:
                    case FloatingPoint:
                    case Utf8:
                    case LargeUtf8:
                    case Struct:
                    case List:
                    case LargeList:
                    case FixedSizeList:
                    case Union:
                    case Map:
                    case Interval:
                    case Binary:
                    case LargeBinary:
                    case FixedSizeBinary:
                    case NONE:
                    case Duration:
                        return false;
                    case Date:
                    case Timestamp:
                    case Time:
                        return true;
                }
            }
            case Interval:
            case Duration:

            case NONE:
                break;
        }
        return false;
    }

    public static boolean isNumber(ArrowType type) {
        return false;
    }

    public static InnerType castToAggType(InnerType type) {
        switch (type) {
            case BOOLEAN_TYPE:
            case INT8_TYPE:
            case INT16_TYPE:
            case CHAR_TYPE:
            case INT32_TYPE:
            case INT64_TYPE:
            case UINT8_TYPE:
            case UINT16_TYPE:
            case UINT32_TYPE:
            case UINT64_TYPE:
            case DATE_TYPE:
            case DATETIME_MILLI_TYPE:
            case TIME_MILLI_TYPE:
                return INT64_TYPE;
            case FLOAT_TYPE:
            case DOUBLE_TYPE:
            case STRING_TYPE:
            case SYMBOL_TYPE:
            case BINARY_TYPE:
            case NULL_TYPE:
            case OBJECT_TYPE:
            default:
                return DOUBLE_TYPE;
        }
    }

    public static ArrowType castToAggType(ArrowType type) {
        return InnerType.from(type).getArrowType();
    }

    public static InnerType[] fromSchemaToInnerTypes(Schema schema) {
        List<Field> fields = schema.getFields();
        InnerType[] innerTypes = new InnerType[fields.size()];

        int index = 0;
        for (Field field : fields) {
            innerTypes[index] = InnerType.from(field.getType());
            ++index;
        }

        return innerTypes;
    }

    public static IntInnerType[] fromSchemaToIntInnerTypes(Schema schema) {
        List<Field> fields = schema.getFields();
        int index = 0;
        IntInnerType[] intPairs = new IntInnerType[fields.size()];
        for (Field field : fields) {
            InnerType innerType = InnerType.from(field.getType());
            intPairs[index] = IntInnerType.of(index, innerType);
            ++index;
        }
        return intPairs;
    }

    public static IntInnerType[] fromSchemaToIntInnerTypes(List<Integer> indexes, Schema schema) {
        List<Field> fields = schema.getFields();
        IntInnerType[] intPairs = new IntInnerType[indexes.size()];
        int seq = 0;
        for (Integer index : indexes) {
            InnerType innerType = InnerType.from(fields.get(index).getType());
            intPairs[seq++] = IntInnerType.of(index, innerType);
        }
        return intPairs;
    }

    public static InnerType fromJdbc(int columnType) {
        switch (JDBCType.valueOf(columnType)) {
            case BOOLEAN:
            case BIT:
                return InnerType.BOOLEAN_TYPE;
            case TINYINT:
                return InnerType.INT8_TYPE;
            case SMALLINT:
                return InnerType.INT16_TYPE;
            case INTEGER:
                return InnerType.INT32_TYPE;
            case BIGINT:
                return InnerType.INT64_TYPE;
            case FLOAT:
            case REAL:
                return InnerType.FLOAT_TYPE;
            case DOUBLE:
                return InnerType.DOUBLE_TYPE;
            case NUMERIC:
            case DECIMAL:
                return InnerType.DECIMAL_TYPE;
            case CHAR:
                return InnerType.CHAR_TYPE;
            case VARCHAR:
            case LONGVARCHAR:
                return InnerType.STRING_TYPE;
            case DATE:
                return InnerType.DATE_TYPE;
            case TIME:
                return InnerType.TIME_MILLI_TYPE;
            case TIMESTAMP_WITH_TIMEZONE:
            case TIMESTAMP:
                return InnerType.DATETIME_MILLI_TYPE;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                return InnerType.BINARY_TYPE;
            case NULL:
                return InnerType.NULL_TYPE;
            case OTHER:
            case JAVA_OBJECT:
            case DISTINCT:
            case STRUCT:
            case ARRAY:
                return InnerType.STRING_TYPE;
            case BLOB:
                return InnerType.BINARY_TYPE;
            case CLOB:
                return InnerType.STRING_TYPE;
            case REF:
            case DATALINK:
            case ROWID:
                return InnerType.STRING_TYPE;
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case NCLOB:
                return InnerType.STRING_TYPE;
            case SQLXML:
            case REF_CURSOR:
            case TIME_WITH_TIMEZONE:
                return InnerType.STRING_TYPE;
            default:
                throw new IllegalStateException("Unexpected value: " + JDBCType.valueOf(columnType));
        }
    }

   public InnerType toUnsigned() {
        switch (this) {
            case BOOLEAN_TYPE:
            case CHAR_TYPE:
            case DATE_TYPE:
            case TIME_MILLI_TYPE:
            case UINT64_TYPE:
            case UINT32_TYPE:
            case UINT16_TYPE:
            case UINT8_TYPE:
            case BINARY_TYPE:
            case STRING_TYPE:
            case DECIMAL_TYPE:
            case DOUBLE_TYPE:
            case FLOAT_TYPE:
            case DATETIME_MILLI_TYPE:
            case SYMBOL_TYPE:
            case OBJECT_TYPE:
            case NULL_TYPE:
                return this;
            case INT8_TYPE:
                return UINT8_TYPE;
            case INT16_TYPE:
                return UINT16_TYPE;
            case INT32_TYPE:
                return UINT32_TYPE;
            case INT64_TYPE:
                return UINT64_TYPE;
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }
}
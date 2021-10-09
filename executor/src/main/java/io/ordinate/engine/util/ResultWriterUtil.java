package io.ordinate.engine.util;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.resultset.ResultSetWriter;
import io.ordinate.engine.schema.InnerType;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;

public class ResultWriterUtil {
    public  static void vectorRowBatchToResultSetWriter(VectorSchemaRoot vectorRowBatch,
                                                 ResultSetWriter newWriter,
                                                 final InnerType[] types,
                                                 final int rowId) {
        final int fieldCount = types.length;
        newWriter.startNewRow(fieldCount);
        for (int fieldId = 0; fieldId < fieldCount; fieldId++) {
            FieldVector vector = vectorRowBatch.getVector(fieldId);
            newWriter.addFlagNull(vector.isNull(fieldId));
        }
        newWriter.endNullMap();
        for (int fieldId = 0; fieldId < fieldCount; fieldId++) {
            FieldVector vector = vectorRowBatch.getVector(fieldId);
            InnerType type = types[fieldId];
            if (!vector.isNull(rowId)) {
                switch (type) {
                    case BOOLEAN_TYPE: {
                        BitVector bitVector = (BitVector) vector;
                        newWriter.addBoolean(bitVector.get(rowId));
                        break;
                    }
                    case INT8_TYPE: {
                        TinyIntVector tinyIntVector = (TinyIntVector) vector;
                        newWriter.addInt8(tinyIntVector.get(rowId));
                        break;
                    }

                    case INT16_TYPE: {
                        SmallIntVector smallIntVector = (SmallIntVector) vector;
                        newWriter.addInt16(smallIntVector.get(rowId));
                        break;
                    }
                    case CHAR_TYPE: {
                        UInt2Vector smallIntVector = (UInt2Vector) vector;
                        newWriter.addChar(smallIntVector.get(rowId));
                        break;
                    }
                    case INT32_TYPE: {
                        IntVector intVector = (IntVector) vector;
                        newWriter.addInt32(intVector.get(rowId));
                        break;
                    }
                    case INT64_TYPE: {
                        BigIntVector intVector = (BigIntVector) vector;
                        newWriter.addInt64(intVector.get(rowId));
                        break;
                    }
                    case FLOAT_TYPE: {
                        Float4Vector float4Vector = (Float4Vector) vector;
                        newWriter.addFloat(float4Vector.get(rowId));
                        break;
                    }
                    case DOUBLE_TYPE: {
                        Float8Vector float8Vector = (Float8Vector) vector;
                        newWriter.addDouble(float8Vector.get(rowId));
                        break;
                    }
                    case STRING_TYPE: {
                        VarCharVector varCharVector = (VarCharVector) vector;
                        newWriter.addString(varCharVector.get(rowId));
                        break;
                    }
                    case BINARY_TYPE: {
                        VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
                        newWriter.addBinary(varBinaryVector.get(rowId));
                        break;
                    }
                    case UINT8_TYPE: {
                        UInt1Vector uInt1Vector = (UInt1Vector) vector;
                        newWriter.addUInt8(uInt1Vector.get(rowId));
                        break;
                    }
                    case UINT16_TYPE: {
                        UInt2Vector uInt2Vector = (UInt2Vector) vector;
                        newWriter.addUInt16((short) uInt2Vector.get(rowId));
                        break;
                    }
                    case UINT32_TYPE: {

                        UInt4Vector uInt4Vector = (UInt4Vector) vector;
                        newWriter.addUInt32(uInt4Vector.get(rowId));
                        break;
                    }
                    case UINT64_TYPE: {

                        UInt8Vector uInt4Vector = (UInt8Vector) vector;
                        newWriter.addUInt64(uInt4Vector.get(rowId));
                        break;
                    }
                    case TIME_MILLI_TYPE: {

                        TimeMilliVector timeMilliVector = (TimeMilliVector) vector;
                        newWriter.addTime(timeMilliVector.get(rowId));
                        break;
                    }
                    case DATE_TYPE: {

                        DateMilliVector datemilliVector = (DateMilliVector) vector;
                        newWriter.addDate(datemilliVector.get(rowId));
                        break;
                    }
                    case DATETIME_MILLI_TYPE: {

                        TimeStampMilliVector datetimeMilliVector = (TimeStampMilliVector) vector;
                        newWriter.addDatetime(datetimeMilliVector.get(rowId));
                        break;
                    }
                    case SYMBOL_TYPE:
                    case OBJECT_TYPE: {
                        VarCharVector varCharVector = (VarCharVector) vector;
                        newWriter.addString(varCharVector.get(rowId));
                        break;
                    }
                    case NULL_TYPE: {
                        throw new UnsupportedOperationException();
                    }
                }
            }
            byte[] build = newWriter.build();
        }
    }

    public static MycatRowMetaData vectorRowBatchToResultSetColumn(Schema schema) {

        return null;
    }
}

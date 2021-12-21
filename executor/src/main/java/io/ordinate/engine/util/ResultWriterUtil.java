package io.ordinate.engine.util;

import com.google.common.collect.ImmutableList;
import io.mycat.Datetimes;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.resultset.ResultSetWriter;
import io.ordinate.engine.builder.SchemaBuilder;
import io.ordinate.engine.schema.FieldBuilder;
import io.ordinate.engine.schema.InnerType;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.sql.Date;
import java.sql.Time;
import java.util.Iterator;
import java.util.List;

public class ResultWriterUtil {
    public static Observable<Object[]> vectorRowBatchToJdbcRowObject(InnerType[] innerTypes, VectorSchemaRoot vectorRowBatch) {
        return Observable.create(new ObservableOnSubscribe<Object[]>() {
            @Override
            public void subscribe(@NonNull ObservableEmitter<Object[]> emitter) throws Throwable {
                final int fieldCount = innerTypes.length;
                int rowCount = vectorRowBatch.getRowCount();
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    Object[] objects = new Object[fieldCount];
                    for (int fieldId = 0; fieldId < fieldCount; fieldId++) {
                        InnerType type = innerTypes[fieldId];
                        FieldVector vector = vectorRowBatch.getVector(fieldId);
                        Object o = null;
                        if (!vector.isNull(rowId)) {
                            switch (type) {
                                case BOOLEAN_TYPE: {
                                    BitVector bitVector = (BitVector) vector;
                                    o = getJdbcBooleanValue(rowId, bitVector);
                                    break;
                                }
                                case INT8_TYPE: {
                                    TinyIntVector tinyIntVector = (TinyIntVector) vector;
                                    o = getJdbcInt8Value(rowId, tinyIntVector);
                                    break;
                                }

                                case INT16_TYPE: {
                                    SmallIntVector smallIntVector = (SmallIntVector) vector;
                                    o = getJdbcInt16Value(rowId, smallIntVector);
                                    break;
                                }
                                case CHAR_TYPE: {
                                    UInt2Vector smallIntVector = (UInt2Vector) vector;
                                    o = getJdbcCharValue(rowId, smallIntVector);
                                    break;
                                }
                                case INT32_TYPE: {
                                    IntVector intVector = (IntVector) vector;
                                    o = getJdbcInt32Value(rowId, intVector);
                                    break;
                                }
                                case INT64_TYPE: {
                                    BigIntVector intVector = (BigIntVector) vector;
                                    o = getJdbcInt64Value(rowId, intVector);
                                    break;
                                }
                                case FLOAT_TYPE: {
                                    Float4Vector float4Vector = (Float4Vector) vector;
                                    o = getJdbcFloatValue(rowId, float4Vector);
                                    break;
                                }
                                case DOUBLE_TYPE: {
                                    Float8Vector float8Vector = (Float8Vector) vector;
                                    o = getJdbcDoubleValue(rowId, float8Vector);
                                    break;
                                }
                                case STRING_TYPE: {
                                    VarCharVector varCharVector = (VarCharVector) vector;
                                    o = getJavaStringValue(rowId, varCharVector);
                                    break;
                                }
                                case BINARY_TYPE: {
                                    VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
                                    o = getJavaBinaryArrayValue(rowId, varBinaryVector);
                                    break;
                                }
                                case UINT8_TYPE: {
                                    UInt1Vector uInt1Vector = (UInt1Vector) vector;
                                    o = getJavaUint8Value(rowId, uInt1Vector);
                                    break;
                                }
                                case UINT16_TYPE: {
                                    UInt2Vector uInt2Vector = (UInt2Vector) vector;
                                    o = getJavaUint16Value(rowId, uInt2Vector);
                                    break;
                                }
                                case UINT32_TYPE: {
                                    UInt4Vector uInt4Vector = (UInt4Vector) vector;
                                    o = getJavaUint32Value(rowId, uInt4Vector);
                                    break;
                                }
                                case UINT64_TYPE: {
                                    UInt8Vector uInt4Vector = (UInt8Vector) vector;
                                    o = getJavaUint64Value(rowId, uInt4Vector);
                                    break;
                                }
                                case TIME_MILLI_TYPE: {
                                    TimeMilliVector timeMilliVector = (TimeMilliVector) vector;
                                    o = getJavaDateMillsValueAsTime(rowId, timeMilliVector);
                                    break;
                                }
                                case DATE_TYPE: {
                                    DateMilliVector datemilliVector = (DateMilliVector) vector;
                                    o = getJavaDateMillsValueAsDate(rowId, datemilliVector);
                                    break;
                                }
                                case DATETIME_MILLI_TYPE: {
                                    TimeStampMilliVector datetimeMilliVector = (TimeStampMilliVector) vector;
                                    o = getJavaDateMillsValueAsDatetime(rowId, datetimeMilliVector);
                                    break;
                                }
                                case SYMBOL_TYPE:
                                case OBJECT_TYPE: {
                                    VarCharVector varCharVector = (VarCharVector) vector;
                                    o = getJavaStringValue(rowId, varCharVector);
                                    break;
                                }
                                case NULL_TYPE: {
                                    o = null;
                                    break;
                                }
                            }
                        } else {

                        }
                        objects[fieldId] = o;
                    }
                    emitter.onNext(objects);
                }

                emitter.onComplete();
            }
        });
    }

    private static Object getJavaDateMillsValueAsDatetime(int rowId, TimeStampMilliVector datetimeMilliVector) {
        long l = datetimeMilliVector.get(rowId);
        return Datetimes.toJavaDate(l);
    }

    private static Date getJavaDateMillsValueAsDate(int rowId, DateMilliVector datemilliVector) {
        long i = datemilliVector.get(rowId);
        return Datetimes.toJavaDate(i);
    }

    private static Time getJavaDateMillsValueAsTime(int rowId, TimeMilliVector timeMilliVector) {
        int i = timeMilliVector.get(rowId);
        return Datetimes.toJavaTime(i);
    }

    public static void vectorRowBatchToResultSetWriter(VectorSchemaRoot vectorRowBatch,
                                                       ResultSetWriter newWriter,
                                                       final InnerType[] types,
                                                       final int rowId) {
        final int fieldCount = types.length;
        newWriter.startNewRow(fieldCount);
        for (int fieldId = 0; fieldId < fieldCount; fieldId++) {
            FieldVector vector = vectorRowBatch.getVector(fieldId);
            InnerType type = types[fieldId];
            if (!vector.isNull(rowId)) {
                switch (type) {
                    case BOOLEAN_TYPE: {
                        BitVector bitVector = (BitVector) vector;
                        newWriter.addBoolean(getJdbcBooleanValue(rowId, bitVector)>0);
                        break;
                    }
                    case INT8_TYPE: {
                        TinyIntVector tinyIntVector = (TinyIntVector) vector;
                        newWriter.addInt8(getJdbcInt8Value(rowId, tinyIntVector));
                        break;
                    }

                    case INT16_TYPE: {
                        SmallIntVector smallIntVector = (SmallIntVector) vector;
                        newWriter.addInt16(getJdbcInt16Value(rowId, smallIntVector));
                        break;
                    }
                    case CHAR_TYPE: {
                        UInt2Vector smallIntVector = (UInt2Vector) vector;
                        newWriter.addChar(getJdbcCharValue(rowId, smallIntVector));
                        break;
                    }
                    case INT32_TYPE: {
                        IntVector intVector = (IntVector) vector;
                        newWriter.addInt32(getJdbcInt32Value(rowId, intVector));
                        break;
                    }
                    case INT64_TYPE: {
                        BigIntVector bigIntVector = (BigIntVector) vector;
                        newWriter.addInt64(getJdbcInt64Value(rowId, bigIntVector));
                        break;
                    }
                    case FLOAT_TYPE: {
                        Float4Vector float4Vector = (Float4Vector) vector;
                        newWriter.addFloat(getJdbcFloatValue(rowId, float4Vector));
                        break;
                    }
                    case DOUBLE_TYPE: {
                        Float8Vector float8Vector = (Float8Vector) vector;
                        newWriter.addDouble(getJdbcDoubleValue(rowId, float8Vector));
                        break;
                    }
                    case STRING_TYPE: {
                        VarCharVector varCharVector = (VarCharVector) vector;
                        newWriter.addString(getJavaStringByteArrayValue(rowId, varCharVector));
                        break;
                    }
                    case BINARY_TYPE: {
                        VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
                        newWriter.addBinary(getJavaBinaryArrayValue(rowId, varBinaryVector));
                        break;
                    }
                    case UINT8_TYPE: {
                        UInt1Vector uInt1Vector = (UInt1Vector) vector;
                        newWriter.addUInt8(getJavaUint8Value(rowId, uInt1Vector));
                        break;
                    }
                    case UINT16_TYPE: {
                        UInt2Vector uInt2Vector = (UInt2Vector) vector;
                        newWriter.addUInt16(getJavaUint16Value(rowId, uInt2Vector));
                        break;
                    }
                    case UINT32_TYPE: {

                        UInt4Vector uInt4Vector = (UInt4Vector) vector;
                        newWriter.addUInt32(getJavaUint32Value(rowId, uInt4Vector));
                        break;
                    }
                    case UINT64_TYPE: {

                        UInt8Vector uInt4Vector = (UInt8Vector) vector;
                        newWriter.addUInt64(getJavaUint64Value(rowId, uInt4Vector));
                        break;
                    }
                    case TIME_MILLI_TYPE: {

                        TimeMilliVector timeMilliVector = (TimeMilliVector) vector;
                        newWriter.addTime(getJavaTimeMillsValueAsInt(rowId, timeMilliVector));
                        break;
                    }
                    case DATE_TYPE: {

                        DateMilliVector datemilliVector = (DateMilliVector) vector;
                        newWriter.addDate(getJavaDateMillsValueAsLong(rowId, datemilliVector));
                        break;
                    }
                    case DATETIME_MILLI_TYPE: {

                        TimeStampMilliVector datetimeMilliVector = (TimeStampMilliVector) vector;
                        newWriter.addDatetime(getJavaDatetimeMilliValueAsLong(rowId, datetimeMilliVector));
                        break;
                    }
                    case SYMBOL_TYPE:
                    case OBJECT_TYPE: {
                        VarCharVector varCharVector = (VarCharVector) vector;
                        newWriter.addString(getJavaStringByteArrayValue(rowId, varCharVector));
                        break;
                    }
                    case NULL_TYPE: {
                        throw new UnsupportedOperationException();
                    }
                }
            }else {
                newWriter.addFlagNull(true);
            }
        }
    }

    private static long getJavaDatetimeMilliValueAsLong(int rowId, TimeStampMilliVector datetimeMilliVector) {
        return datetimeMilliVector.get(rowId);
    }

    private static long getJavaDateMillsValueAsLong(int rowId, DateMilliVector datemilliVector) {
        return datemilliVector.get(rowId);
    }

    private static int getJavaTimeMillsValueAsInt(int rowId, TimeMilliVector timeMilliVector) {
        return timeMilliVector.get(rowId);
    }

    private static long getJavaUint64Value(int rowId, UInt8Vector uInt4Vector) {
        return uInt4Vector.get(rowId);
    }

    private static int getJavaUint32Value(int rowId, UInt4Vector uInt4Vector) {
        return uInt4Vector.get(rowId);
    }

    private static short getJavaUint16Value(int rowId, UInt2Vector uInt2Vector) {
        return (short) getJdbcCharValue(rowId, uInt2Vector);
    }

    private static byte getJavaUint8Value(int rowId, UInt1Vector uInt1Vector) {
        return uInt1Vector.get(rowId);
    }

    private static byte[] getJavaBinaryArrayValue(int rowId, VarBinaryVector varBinaryVector) {
        return varBinaryVector.get(rowId);
    }

    private static String getJavaStringValue(int rowId, VarCharVector varCharVector) {
        return new String(varCharVector.get(rowId));
    }

    private static byte[] getJavaStringByteArrayValue(int rowId, VarCharVector varCharVector) {
        return varCharVector.get(rowId);
    }

    private static double getJdbcDoubleValue(int rowId, Float8Vector float8Vector) {
        return float8Vector.get(rowId);
    }

    private static float getJdbcFloatValue(int rowId, Float4Vector float4Vector) {
        return float4Vector.get(rowId);
    }

    private static long getJdbcInt64Value(int rowId, BigIntVector bigIntVector) {
        return bigIntVector.get(rowId);
    }

    private static int getJdbcInt32Value(int rowId, IntVector intVector) {
        return intVector.get(rowId);
    }

    private static char getJdbcCharValue(int rowId, UInt2Vector smallIntVector) {
        return smallIntVector.get(rowId);
    }

    private static short getJdbcInt16Value(int rowId, SmallIntVector smallIntVector) {
        return smallIntVector.get(rowId);
    }

    private static byte getJdbcInt8Value(int rowId, TinyIntVector tinyIntVector) {
        return tinyIntVector.get(rowId);
    }

    private static int getJdbcBooleanValue(int rowId, BitVector bitVector) {
        return bitVector.get(rowId);
    }

    public static MycatRowMetaData vectorRowBatchToResultSetColumn(Schema schema) {
        ResultSetBuilder writer = ResultSetBuilder.create();
        List<Field> fields = schema.getFields();
        for (Field field : fields) {
            InnerType innerType = InnerType.from(field.getType());
            writer.addColumnInfo(field.getName(), innerType.getJdbcType(), field.isNullable(), innerType.isSigned());
        }
        return writer.build().getMetaData();
    }

    public static Schema resultSetColumnToVectorRowSchema(MycatRowMetaData mycatRowMetaData) {
        int columnCount = mycatRowMetaData.getColumnCount();
        ImmutableList.Builder<Field> builder = ImmutableList.builder();
        for (int i = 0; i < columnCount; i++) {
            String columnName = mycatRowMetaData.getColumnName(i);
            int columnType = mycatRowMetaData.getColumnType(i);
            boolean signed = mycatRowMetaData.isSigned(i);
            boolean nullable = mycatRowMetaData.isNullable(i);
            InnerType innerType = InnerType.fromJdbc(columnType);
            if (!signed) {
                innerType = innerType.toUnsigned();
            }
            Field field = FieldBuilder.of(columnName, innerType.getArrowType(), nullable).toArrow();
            builder.add(field);
        }
        return new org.apache.arrow.vector.types.pojo.Schema(builder.build());
    }

    public static Observable<VectorSchemaRoot> convertToVector(RowBaseIterator rowBaseIterator) {
        return Observable.create(emitter -> {
            try {
                RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
                MycatRowMetaData metaData = rowBaseIterator.getMetaData();
                Schema schema = ResultWriterUtil.resultSetColumnToVectorRowSchema(metaData);
                VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
                int columnCount = metaData.getColumnCount();

                List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
                vectorSchemaRoot.allocateNew();
                int rowId = 0;
                while (rowBaseIterator.next()) {
                    for (int i = 0; i < columnCount; i++) {
                        FieldVector valueVectors = fieldVectors.get(i);
                        Object object = rowBaseIterator.getObject(i);
                        if (object == null) {
                            SchemaBuilder.setVectorNull(valueVectors, rowId);
                        } else {
                            SchemaBuilder.setVector(valueVectors, rowId, object);
                        }
                    }
                    rowId++;
                }
                vectorSchemaRoot.setRowCount(rowId);
                emitter.onNext(vectorSchemaRoot);
                emitter.onComplete();
            } catch (Throwable e) {
                emitter.tryOnError(e);
            }
        });
    }

    public static Observable<VectorSchemaRoot> convertToVector(Schema schema, Observable<Object[]> observable) {
        return Observable.create(emitter -> {
            try {
                RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
                int columnCount = schema.getFields().size();

                List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
                vectorSchemaRoot.allocateNew();
                int rowId = 0;
                Iterator<Object[]> rowBaseIterator = observable.blockingIterable().iterator();
                while (rowBaseIterator.hasNext()) {
                    Object[] objects = rowBaseIterator.next();
                    for (int i = 0; i < columnCount; i++) {
                        FieldVector valueVectors = fieldVectors.get(i);
                        Object object = objects[(i)];
                        if (object == null) {
                            SchemaBuilder.setVectorNull(valueVectors, rowId);
                        } else {
                            SchemaBuilder.setVector(valueVectors, rowId, object);
                        }
                    }
                    rowId++;
                }
                vectorSchemaRoot.setRowCount(rowId);
                emitter.onNext(vectorSchemaRoot);
                emitter.onComplete();
            } catch (Throwable e) {
                emitter.tryOnError(e);
            }
        });
    }
}

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

import com.google.common.collect.ImmutableList;
import io.ordinate.engine.function.Function;
import io.mycat.beans.mycat.ArrowTypes;
import io.ordinate.engine.schema.FieldBuilder;
import io.ordinate.engine.schema.InnerType;
import lombok.ToString;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@ToString
public class SchemaBuilder {
    final List<FieldBuilder> fields;

    public SchemaBuilder(List<FieldBuilder> fields) {
        this.fields = fields;
    }

    public static SchemaBuilder ofArrowType(ArrowType... types) {
        return ofArrowType(Arrays.asList(types));
    }

    public static SchemaBuilder ofArrowType(Iterable<ArrowType> types) {
        int index = 0;
        ImmutableList.Builder<FieldBuilder> builder = ImmutableList.builder();
        for (ArrowType type : types) {
            builder.add(FieldBuilder.of("$" + index, Objects.requireNonNull(type), true));
        }
        return new SchemaBuilder(builder.build());
    }

    public static Schema of(Function[] functions) {
        final Schema schema;
        ArrowType[] arrowTypes = new ArrowType[functions.length];
        int index = 0;
        for (Function function : functions) {
            arrowTypes[index] = function.getType().getArrowType();
            index++;
        }
        schema = SchemaBuilder.ofArrowType(arrowTypes).toArrow();
        return schema;
    }

    public static void copyTo(VectorSchemaRoot leftInput, int leftRowId, VectorSchemaRoot output, int outputIndex) {
        int columnCount = leftInput.getSchema().getFields().size();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            output.getVector(columnIndex).copyFrom(leftRowId, outputIndex, leftInput.getVector(columnIndex));
        }
    }

    public static void joinCopyTo(VectorSchemaRoot leftInput, int leftRowId, VectorSchemaRoot rightInput, int rightRowIndex, VectorSchemaRoot output, int outputIndex) {
        int columnCount = output.getSchema().getFields().size();
        int leftCount = leftInput.getSchema().getFields().size();

        for (int i = 0; i < leftCount; i++) {
            output.getVector(i).copyFrom(leftRowId, outputIndex, leftInput.getVector(i));
        }
        for (int i = leftCount; i < columnCount; i++) {
            output.getVector(i).copyFrom(rightRowIndex, outputIndex, rightInput.getVector(i - leftCount));
        }
    }

    public static org.apache.arrow.vector.types.pojo.Schema ofInnerTypes(List<InnerType> innerTypes) {
        ArrayList<ArrowType> types = new ArrayList<>();
        for (InnerType innerType : innerTypes) {
            ArrowType arrowType = innerType.getArrowType();
            types.add(arrowType);
        }
        return ofArrowType(types).toArrow();

    }

    public static int getFieldCount(VectorSchemaRoot root) {
        return root.getSchema().getFields().size();
    }

    public static InnerType[] getInnerTypes(VectorSchemaRoot vectorRowBatch) {
        int fieldCount = getFieldCount(vectorRowBatch);
        InnerType[] types = new InnerType[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            types[i]= InnerType.from(vectorRowBatch.getVector(i).getField().getType());
        }
        return types;
    }

    public org.apache.arrow.vector.types.pojo.Schema toArrow() {
        ImmutableList.Builder<org.apache.arrow.vector.types.pojo.Field> builder = ImmutableList.builder();
        for (FieldBuilder field : fields) {
            builder.add(field.toArrow());
        }
        return new org.apache.arrow.vector.types.pojo.Schema(builder.build());
    }

    public int size() {
        return fields.size();
    }

    public static void setVector(FieldVector vector, int index, String value) {
        FieldType fieldType = vector.getField().getFieldType();
        ArrowType type = fieldType.getType();
        if (type == ArrowTypes.INT8_TYPE) {
            TinyIntVector valueVectors = (TinyIntVector) vector;
            valueVectors.set(index, Integer.parseInt(value));
        } else if (type == ArrowTypes.INT16_TYPE) {
            SmallIntVector valueVectors = (SmallIntVector) vector;
            valueVectors.set(index, Integer.parseInt(value));
        } else if (type == ArrowTypes.INT32_TYPE) {
            IntVector valueVectors = (IntVector) vector;
            valueVectors.set(index, Integer.parseInt(value));
        } else if (type == ArrowTypes.INT64_TYPE) {
            BigIntVector valueVectors = (BigIntVector) vector;
            valueVectors.set(index, Long.parseLong(value));
        } else if (type == ArrowTypes.FLOAT_TYPE) {
            Float4Vector valueVectors = (Float4Vector) vector;
            valueVectors.set(index, Float.parseFloat(value));
        } else if (type == ArrowTypes.DOUBLE_TYPE) {
            Float8Vector valueVectors = (Float8Vector) vector;
            valueVectors.set(index, Double.parseDouble(value));
        } else if (type == ArrowTypes.STRING_TYPE) {
            VarCharVector valueVectors = (VarCharVector) vector;
            valueVectors.setSafe(index, new Text(value));
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static void setVector(FieldVector vector, int index, Object value) {
        if (value == null) {
            if (vector instanceof BaseFixedWidthVector) {
                BaseFixedWidthVector baseFixedWidthVector = (BaseFixedWidthVector) vector;
                baseFixedWidthVector.setNull(index);
            } else if (vector instanceof BaseVariableWidthVector) {
                BaseVariableWidthVector baseVariableWidthVector = (BaseVariableWidthVector) vector;
                baseVariableWidthVector.setNull(index);
            }
            return;
        }
        FieldType fieldType = vector.getField().getFieldType();
        ArrowType type = fieldType.getType();
        if (type == ArrowTypes.INT8_TYPE) {
            TinyIntVector valueVectors = (TinyIntVector) vector;

            valueVectors.set(index, ((Number) (value)).intValue());
        } else if (type == ArrowTypes.INT16_TYPE) {
            SmallIntVector valueVectors = (SmallIntVector) vector;
            valueVectors.set(index,  ((Number) (value)).intValue());
        } else if (type == ArrowTypes.INT32_TYPE) {
            IntVector valueVectors = (IntVector) vector;
            if (value instanceof String){
                value = Long.parseLong(((String) value));
            }
            valueVectors.set(index, ((Number) (value)).intValue());
        } else if (type == ArrowTypes.INT64_TYPE) {
            BigIntVector valueVectors = (BigIntVector) vector;
            valueVectors.set(index, (Long) (value));
        } else if (type == ArrowTypes.FLOAT_TYPE) {
            Float4Vector valueVectors = (Float4Vector) vector;
            valueVectors.set(index, (Float) (value));
        } else if (type == ArrowTypes.DOUBLE_TYPE) {
            Float8Vector valueVectors = (Float8Vector) vector;
            valueVectors.set(index, (Double) (value));
        } else if (type == ArrowTypes.STRING_TYPE) {
            VarCharVector valueVectors = (VarCharVector) vector;
            if (value instanceof String) {
                valueVectors.setSafe(index, new Text((String) value));
            } else if (value instanceof byte[]) {
                valueVectors.setSafe(index, new Text((byte[]) value));
            } else if (value instanceof Text) {
                valueVectors.setSafe(index, (Text) value);
            } else {
                throw new UnsupportedOperationException();
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static void setVectorNull(FieldVector vector, int index) {
        if (vector instanceof BaseVariableWidthVector) {
            ((BaseVariableWidthVector) vector).setNull(index);
        } else if (vector instanceof BaseFixedWidthVector) {
            ((BaseFixedWidthVector) vector).setNull(index);
        } else {
            throw new IllegalArgumentException();
        }
    }
}

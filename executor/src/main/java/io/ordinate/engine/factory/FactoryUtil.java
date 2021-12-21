package io.ordinate.engine.factory;

import com.google.common.collect.ImmutableList;
import io.mycat.beans.mycat.MycatDataType;
import io.mycat.beans.mycat.MycatField;
import io.mycat.beans.mycat.MycatRelDataType;
import io.ordinate.engine.schema.FieldBuilder;
import io.ordinate.engine.schema.InnerType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public class FactoryUtil {
    public static Schema toArrowSchema(MycatRelDataType mycatRelDataTypeByCalcite) {
        List<MycatField> fieldList = mycatRelDataTypeByCalcite.getFieldList();
        int columnCount = fieldList.size();
        ImmutableList.Builder<org.apache.arrow.vector.types.pojo.Field> builder = ImmutableList.builder();
        for (int i = 0; i < columnCount; i++) {
            MycatField mycatField = fieldList.get(i);
            String columnName = mycatField.getName();
            MycatDataType columnType = mycatField.getMycatDataType();
            boolean nullable = mycatField.isNullable();
            InnerType innerType;
            switch (columnType) {
                case BOOLEAN:
                    innerType = InnerType.BOOLEAN_TYPE;
                    break;
                case BIT:
                case UNSIGNED_LONG:
                    innerType = InnerType.UINT64_TYPE;
                    break;
                case TINYINT:
                    innerType = InnerType.INT8_TYPE;
                    break;
                case UNSIGNED_TINYINT:
                    innerType = InnerType.UINT8_TYPE;
                    break;
                case SHORT:
                    innerType = InnerType.INT16_TYPE;
                    break;
                case UNSIGNED_SHORT:
                case YEAR:
                    innerType = InnerType.UINT16_TYPE;
                    break;
                case INT:
                    innerType = InnerType.INT32_TYPE;
                    break;
                case UNSIGNED_INT:
                    innerType = InnerType.UINT32_TYPE;
                    break;
                case LONG:
                    innerType = InnerType.INT64_TYPE;
                    break;
                case DOUBLE:
                    innerType = InnerType.DOUBLE_TYPE;
                    break;
                case DECIMAL:
                    innerType = InnerType.DECIMAL_TYPE;
                    break;
                case DATE:
                    innerType = InnerType.DATE_TYPE;
                    break;
                case DATETIME:
                    innerType = InnerType.DATETIME_MILLI_TYPE;
                    break;
                case TIME:
                    innerType = InnerType.TIME_MILLI_TYPE;
                    break;
                case CHAR:
                case VARCHAR:
                    innerType = InnerType.STRING_TYPE;
                    break;
                case BINARY:
                    innerType = InnerType.BINARY_TYPE;
                    break;
                case FLOAT:
                    innerType = InnerType.FLOAT_TYPE;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + columnType);
            }
            org.apache.arrow.vector.types.pojo.Field field = FieldBuilder.of(columnName, innerType.getArrowType(), nullable).toArrow();
            builder.add(field);
        }

        return new Schema(builder.build());
    }
}

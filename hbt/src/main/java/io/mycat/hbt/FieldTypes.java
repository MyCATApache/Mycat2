package io.mycat.hbt;

import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.hbt.ast.base.FieldType;
import lombok.SneakyThrows;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FieldTypes {
    @SneakyThrows
    public static List<FieldType> getFieldTypes(ResultSetMetaData metaData) {
        return getFieldTypes(new JdbcRowMetaData(metaData));
    }

    @SneakyThrows
    public static List<FieldType> getFieldTypes(MycatRowMetaData metaData) {
        int columnCount = metaData.getColumnCount();
        ArrayList<FieldType> fieldTypes = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            final String columnName = metaData.getColumnName(i);
            SqlTypeName sqlTypeName = Objects.requireNonNull(HBTCalciteSupport.INSTANCE.getSqlTypeByJdbcValue(metaData.getColumnType(i)), "type is not existed,类型不存在");
            final String columnType = sqlTypeName.getName();
            final boolean nullable = metaData.isNullable(i);
            final Integer precision = metaData.getPrecision(i);
            final Integer scale = metaData.getScale(i);
            fieldTypes.add(new FieldType(columnName, columnType, nullable, precision, scale));
        }
        return fieldTypes;
    }
}
package io.mycat.calcite;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CalciteConvertors {
    final static List<SimpleColumnInfo> convertfromDatabaseMetaData(DatabaseMetaData databaseMetaData, String catalog, String schema, String tableName) {
        try (ResultSet resultSet = databaseMetaData.getColumns(catalog, schema, tableName, null)) {
            ArrayList<SimpleColumnInfo> res = new ArrayList<>();
            while (resultSet.next()) {
                final String columnName = resultSet.getString(4).toUpperCase();
                final int dataType = resultSet.getInt(5);
                final String typeString = resultSet.getString(6);
                final int precision;
                final int scale;
                switch (SqlType.valueOf(dataType)) {
                    case TIMESTAMP:
                    case TIME:
                        precision = resultSet.getInt(9); // SCALE
                        scale = 0;
                        break;
                    default:
                        precision = resultSet.getInt(7); // SIZE
                        scale = resultSet.getInt(9); // SCALE
                        break;
                }
                boolean nullable = resultSet.getInt(11) != DatabaseMetaData.columnNoNulls;
                res.add(new SimpleColumnInfo(columnName, dataType, precision, scale, typeString, nullable));
            }
            return res;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static RelProtoDataType relDataType(List<SimpleColumnInfo> infos) {
        Objects.requireNonNull(infos);
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        for (SimpleColumnInfo info : infos) {
            RelDataType relDataType = sqlType(typeFactory, info.dataType, info.precision, info.scale, info.typeString);
            fieldInfo.add(info.columnName, relDataType).nullable(info.nullable);
        }
        return RelDataTypeImpl.proto(fieldInfo.build());
    }

    public static RowSignature rowSignature(List<SimpleColumnInfo> infos) {
        Objects.requireNonNull(infos);
        RowSignature.Builder builder = RowSignature.builder();
        for (SimpleColumnInfo info : infos) {
            builder.add(info.columnName, JDBCType.valueOf(info.dataType));
        }
        return builder.build();
    }

    @AllArgsConstructor
    @Getter
    static class SimpleColumnInfo {
        String columnName;
        int dataType;
        int precision;
        int scale;
        String typeString;
        boolean nullable;
    }

    private static RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType,
                                       int precision, int scale, String typeString) {
        // Fall back to ANY if type is unknown
        final SqlTypeName sqlTypeName =
                Util.first(SqlTypeName.getNameForJdbcType(dataType), SqlTypeName.ANY);
        switch (sqlTypeName) {
            case ARRAY:
                RelDataType component = null;
                if (typeString != null && typeString.endsWith(" ARRAY")) {
                    // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type
                    // "INTEGER".
                    final String remaining = typeString.substring(0,
                            typeString.length() - " ARRAY".length());
                    component = parseTypeString(typeFactory, remaining);
                }
                if (component == null) {
                    component = typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.ANY), true);
                }
                return typeFactory.createArrayType(component, -1);
        }
        if (precision >= 0
                && scale >= 0
                && sqlTypeName.allowsPrecScale(true, true)) {
            return typeFactory.createSqlType(sqlTypeName, precision, scale);
        } else if (precision >= 0 && sqlTypeName.allowsPrecNoScale()) {
            return typeFactory.createSqlType(sqlTypeName, precision);
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            return typeFactory.createSqlType(sqlTypeName);
        }
    }

    private static RelDataType parseTypeString(RelDataTypeFactory typeFactory,
                                               String typeString) {
        int precision = -1;
        int scale = -1;
        int open = typeString.indexOf("(");
        if (open >= 0) {
            int close = typeString.indexOf(")", open);
            if (close >= 0) {
                String rest = typeString.substring(open + 1, close);
                typeString = typeString.substring(0, open);
                int comma = rest.indexOf(",");
                if (comma >= 0) {
                    precision = Integer.parseInt(rest.substring(0, comma));
                    scale = Integer.parseInt(rest.substring(comma));
                } else {
                    precision = Integer.parseInt(rest);
                }
            }
        }
        try {
            final SqlTypeName typeName = SqlTypeName.valueOf(typeString);
            return typeName.allowsPrecScale(true, true)
                    ? typeFactory.createSqlType(typeName, precision, scale)
                    : typeName.allowsPrecScale(true, false)
                    ? typeFactory.createSqlType(typeName, precision)
                    : typeFactory.createSqlType(typeName);
        } catch (IllegalArgumentException e) {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.ANY), true);
        }
    }
}
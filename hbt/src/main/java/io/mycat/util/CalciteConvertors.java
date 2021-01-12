/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.util;


import io.mycat.SimpleColumnInfo;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
public class CalciteConvertors {
    private final static Logger LOGGER = LoggerFactory.getLogger(CalciteConvertors.class);


    /**
     * TABLE_CAT:CHAR
     * TABLE_SCHEM:CHAR
     * TABLE_NAME:CHAR
     * COLUMN_NAME:CHAR
     * DATA_TYPE:INT
     * TYPE_NAME:CHAR
     * COLUMN_SIZE:INT
     * BUFFER_LENGTH:INT
     * DECIMAL_DIGITS:INT
     * NUM_PREC_RADIX:INT
     * NULLABLE:INT
     * REMARKS:CHAR
     * COLUMN_DEF:CHAR
     * SQL_DATA_TYPE:INT
     * SQL_DATETIME_SUB:INT
     * CHAR_OCTET_LENGTH:INT
     * ORDINAL_POSITION:INT
     * IS_NULLABLE:CHAR
     * SCOPE_CATALOG:CHAR
     * SCOPE_SCHEMA:CHAR
     * SCOPE_TABLE:CHAR
     * SOURCE_DATA_TYPE:SMALLINT
     * IS_AUTOINCREMENT:CHAR
     * IS_GENERATEDCOLUMN:CHAR
     *
     * @param databaseMetaData
     * @param catalog
     * @param schema
     * @param tableName
     * @return
     */
    public final static List<SimpleColumnInfo> convertfromDatabaseMetaData(DatabaseMetaData databaseMetaData, String catalog, String schema, String tableName) {
        try (ResultSet resultSet = databaseMetaData.getColumns(catalog, schema, tableName, null)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if(LOGGER.isDebugEnabled()){
                    LOGGER.debug(columnName + ":" + metaData.getColumnTypeName(i));
                }


            }

            int id = 0;
            ArrayList<SimpleColumnInfo> res = new ArrayList<>();
            while (resultSet.next()) {

                String TABLE_CAT = resultSet.getString(1);
                String TABLE_SCHEM = resultSet.getString(2);
                String TABLE_NAME = resultSet.getString(3);
                String COLUMN_NAME = resultSet.getString(4);
                int DATA_TYPE = resultSet.getByte(5);
                String TYPE_NAME = resultSet.getString(6);
                int COLUMN_SIZE = resultSet.getInt(7);

                int BUFFER_LENGTH = resultSet.getInt(8);

                int DECIMAL_DIGITS = resultSet.getInt(9);

                int NUM_PREC_RADIX = resultSet.getInt(10);
                int NULLABLE = resultSet.getInt(11);
                String REMARKS = resultSet.getString(12);

                String COLUMN_DEF = resultSet.getString(13);
                int SQL_DATA_TYPE = resultSet.getInt(14);

                int SQL_DATETIME_SUB = resultSet.getInt(15);
                int CHAR_OCTET_LENGTH = resultSet.getInt(16);

                int ORDINAL_POSITION = resultSet.getInt(17);
                boolean IS_NULLABLE = "Yes".equalsIgnoreCase(resultSet.getString(18));
                String SCOPE_CATALOG = resultSet.getString(19);
                String SCOPE_SCHEMA = resultSet.getString(20);
                String SCOPE_TABLE = resultSet.getString(21);

                int SOURCE_DATA_TYPE = resultSet.getInt(22);
                boolean IS_AUTOINCREMENT = "Yes".equalsIgnoreCase(resultSet.getString(23));
                boolean IS_GENERATEDCOLUMN = "Yes".equalsIgnoreCase(resultSet.getString(24));

                final String columnName = resultSet.getString(4);
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
                ;
                JDBCType jdbcType = JDBCType.valueOf(DATA_TYPE);
                //todo 添加索引 主键信息


                res.add(new SimpleColumnInfo(columnName, precision, scale, jdbcType, nullable,
                        IS_AUTOINCREMENT, false, false,id++));
            }
            return res;
        } catch (Throwable e) {
            LOGGER.error("不能从DatabaseMetaData获取字段信息", e);
            return null;
        }
    }

    public static RelProtoDataType relDataType(List<SimpleColumnInfo> infos) {
        Objects.requireNonNull(infos);
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        for (SimpleColumnInfo info : infos) {
            RelDataType relDataType = sqlType(typeFactory, info.getJdbcType().ordinal(), info.getPrecision(), info.getScale(), info.getJdbcType().getName());
            fieldInfo.add(info.getColumnName(), relDataType).nullable(info.isNullable());
        }
        return RelDataTypeImpl.proto(fieldInfo.build());
    }

    private static RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType, int precision, int scale, String typeString) {
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

    private static RelDataType parseTypeString(RelDataTypeFactory typeFactory, String typeString) {
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




    public static List<SimpleColumnInfo> getColumnInfo(MycatRowMetaData mycatRowMetaData) {
        int columnCount = mycatRowMetaData.getColumnCount();
        List<SimpleColumnInfo> list = new ArrayList<>();
        for (int i = 0,id=0; i < columnCount; i++,id++) {
            String columnName = mycatRowMetaData.getColumnName(i);
            int columnType = mycatRowMetaData.getColumnType(i);
            int precision = mycatRowMetaData.getPrecision(i);
            int scale = mycatRowMetaData.getScale(i);
            boolean autoIncrement = mycatRowMetaData.isAutoIncrement(i);
            boolean primaryKey = mycatRowMetaData.isPrimaryKey(i);
            JDBCType jdbcType = JDBCType.valueOf(columnType);
            boolean index = mycatRowMetaData.isIndex(i);
            list.add(new SimpleColumnInfo(columnName, precision, scale, jdbcType,
                    mycatRowMetaData.isNullable(i), autoIncrement, primaryKey, index,id));
        }
        return list;
    }

//
//    public final static Map<String, Map<String, List<SimpleColumnInfo>>> columnInfoListByDataSourceWithCreateTableSQL(final Map<String, Map<String, List<BackendTableInfo>>> schemaBackendMetaMap, Map<String, Map<String, String>> sqlmap) {
//        Map<String, Map<String, List<SimpleColumnInfo>>> schemaColumnMetaMap = new HashMap<>();
//        schemaBackendMetaMap.forEach((schemaName, value) -> {
//            schemaColumnMetaMap.put(schemaName, new HashMap<>());
//            for (Map.Entry<String, List<BackendTableInfo>> stringListEntry : value.entrySet()) {
//                String tableName = stringListEntry.getKey().toLowerCase();
//                List<BackendTableInfo> backs = stringListEntry.getValue();
//                if (backs == null || backs.isEmpty()) return;
//                List<SimpleColumnInfo> info = null;
//                Map<String, String> sqlTableMap = sqlmap.get(schemaName);
//                if (sqlTableMap != null) {
//                    String sql = sqlTableMap.get(tableName);
//                    if (sql != null) {
//                      info = getColumnInfo(sql);
//                    }
//                }
//                if (info==null){
//                    info = getColumnInfo( backs.get(0));
//                }
//                if (info == null) {
//                    schemaColumnMetaMap.remove(tableName);
//                    LOGGER.error("can not fetch {}.{} column info fromTable datasource,may be failure to build targetTable", schemaName, tableName);
//                }else {
//                    schemaColumnMetaMap.get(schemaName).put(tableName, info);
//                }
//            }
//        });
//        return schemaColumnMetaMap;
//    }

//    public final static Map<String, Map<String, List<SimpleColumnInfo>>> columnInfoListBySQL(final Map<String, Map<String, String>> schemaBackendSQL) {
//        Map<String, Map<String, List<SimpleColumnInfo>>> schemaColumnMetaMap = new HashMap<>();
//        schemaBackendSQL.forEach((schemaName, value) -> {
//            schemaColumnMetaMap.put(schemaName, new HashMap<>());
//            for (Map.Entry<String, String> stringListEntry : value.entrySet()) {
//                String tableName = stringListEntry.getKey();
//                String sql = stringListEntry.getValue();
//                if (sql == null || sql.isEmpty()) return;
//                List<SimpleColumnInfo> info = null;
//                info = getColumnInfo(sql);
//                if (info == null) continue;
//                schemaColumnMetaMap.get(schemaName).put(tableName, info);
//            }
//        });
//        return schemaColumnMetaMap;
//    }

    public static List<Pair<ColumnMetaData.Rep, Integer>> fieldClasses(final RelProtoDataType protoRowType,
                                                                       final JavaTypeFactory typeFactory) {
        final RelDataType rowType = protoRowType.apply(typeFactory);
        return rowType.getFieldList().stream().map(f -> {
            final RelDataType type = f.getType();
            final Class clazz = (Class) typeFactory.getJavaClass(type);
            final ColumnMetaData.Rep rep =
                    Util.first(ColumnMetaData.Rep.of(clazz),
                            ColumnMetaData.Rep.OBJECT);
            return Pair.of(rep, type.getSqlTypeName().getJdbcOrdinal());
        }).collect(Collectors.toList());
    }

    public static MycatRowMetaData getMycatRowMetaData(RelDataType rowType) {
        return new CalciteRowMetaData(rowType.getFieldList());

    }

    static class DateConvertor {
        private static Timestamp shift(Timestamp v) {
            if (v == null) {
                return null;
            }
            long time = v.getTime();
            int offset = TimeZone.getDefault().getOffset(time);
            return new Timestamp(time + offset);
        }

        private static Time shift(Time v) {
            if (v == null) {
                return null;
            }
            long time = v.getTime();
            int offset = TimeZone.getDefault().getOffset(time);
            return new Time((time + offset) % DateTimeUtils.MILLIS_PER_DAY);
        }

        private static Date shift(Date v) {
            if (v == null) {
                return null;
            }
            long time = v.getTime();
            int offset = TimeZone.getDefault().getOffset(time);
            return new Date(time + offset);
        }
    }

    public static RelDataType getRelDataType(final List<SimpleColumnInfo> columnInfos, final RelDataTypeFactory factory) {
        final RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(factory);
        for (SimpleColumnInfo columnInfo : columnInfos) {
            final JDBCType columnType = columnInfo.getJdbcType();
            final RelDataType type;
            if (columnType == JDBCType.VARCHAR) {
                type = factory.createTypeWithCharsetAndCollation(
                        factory.createSqlType(SqlTypeName.VARCHAR),
                        Charset.defaultCharset(),
                        SqlCollation.IMPLICIT);
            } else if (columnType == JDBCType.LONGVARBINARY) {
                type = factory.createSqlType(SqlTypeName.VARBINARY);
            } else {
                SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(columnType.getVendorTypeNumber());
                if (sqlTypeName == null) {
                    sqlTypeName = SqlTypeName.VARCHAR;
                }
                type = factory.createSqlType(sqlTypeName);
            }
            builder.add(columnInfo.getColumnName(), factory.createTypeWithNullability(type, columnInfo.isNullable()));
        }
        return builder.build();
    }
}
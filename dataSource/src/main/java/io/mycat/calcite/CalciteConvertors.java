/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.google.common.collect.Lists;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.util.MycatRowMetaDataImpl;
import io.mycat.util.SQL2ResultSetUtil;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.*;
import java.util.*;
/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
public class CalciteConvertors {
    private final static Logger LOGGER = LoggerFactory.getLogger(CalciteConvertors.class);

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

    static List<SimpleColumnInfo> getColumnInfo(BackEndTableInfo tableInfo) {
        List<SimpleColumnInfo> infos;
        DefaultConnection defaultConnection = tableInfo.getSession(true,null);
        try (Connection rawConnection = defaultConnection.getRawConnection()) {
            DatabaseMetaData metaData = rawConnection.getMetaData();
            String schema = tableInfo.getSchemaName();
            infos = CalciteConvertors.convertfromDatabaseMetaData(metaData, schema, schema, tableInfo.getTableName());
        } catch (Exception e) {
            LOGGER.error("", e);
            return null;
        }
        return infos;
    }

    static List<SimpleColumnInfo> getColumnInfo(String sql) {
        MycatRowMetaDataImpl mycatRowMetaData = SQL2ResultSetUtil.getMycatRowMetaData(sql);
        int columnCount = mycatRowMetaData.getColumnCount();
        List<SimpleColumnInfo> list = new ArrayList<>();
        for (int i = 1; i <= columnCount ; i++) {
            String columnName = mycatRowMetaData.getColumnName(i);
            int columnType = mycatRowMetaData.getColumnType(i);
            int precision = mycatRowMetaData.getPrecision(i);
            int scale = mycatRowMetaData.getScale(i);
            String jdbcType = JDBCType.valueOf(columnType).getName();
            list.add(new SimpleColumnInfo(columnName,columnType,precision,scale,jdbcType,true));
        }
        return list;
    }
    public final static Map<String, Map<String, List<SimpleColumnInfo>>> columnInfoListByDataSource(final Map<String, Map<String, List<BackEndTableInfo>>> schemaBackendMetaMap) {
        Map<String, Map<String, List<SimpleColumnInfo>>> schemaColumnMetaMap = new HashMap<>();
        schemaBackendMetaMap.forEach((schemaName, value) -> {
            schemaColumnMetaMap.put(schemaName, new HashMap<>());
            for (Map.Entry<String, List<BackEndTableInfo>> stringListEntry : value.entrySet()) {
                String tableName = stringListEntry.getKey();
                List<BackEndTableInfo> backs = stringListEntry.getValue();
                if (backs == null || backs.isEmpty()) return;
                List<SimpleColumnInfo> info = null;
                for (BackEndTableInfo back : backs) {
                    info = getColumnInfo(back);
                    if (info == null) continue;
                    schemaColumnMetaMap.get(schemaName).put(tableName, info);
                }
                if (info == null) {
                    schemaColumnMetaMap.remove(tableName);
                    LOGGER.error("can not fetch {}.{} column info from datasource,may be failure to build targetTable", schemaName, tableName);
                }
            }
        });
        return schemaColumnMetaMap;
    }
    public final static Map<String, Map<String, List<SimpleColumnInfo>>> columnInfoListBySQL(final Map<String, Map<String,String>> schemaBackendSQL) {
        Map<String, Map<String, List<SimpleColumnInfo>>> schemaColumnMetaMap = new HashMap<>();
        schemaBackendSQL.forEach((schemaName, value) -> {
            schemaColumnMetaMap.put(schemaName, new HashMap<>());
            for (Map.Entry<String, String> stringListEntry : value.entrySet()) {
                String tableName = stringListEntry.getKey();
                String sql= stringListEntry.getValue();
                if (sql == null || sql.isEmpty()) return;
                List<SimpleColumnInfo> info = null;
                    info = getColumnInfo(sql);
                    if (info == null) continue;
                    schemaColumnMetaMap.get(schemaName).put(tableName, info);
            }
        });
        return schemaColumnMetaMap;
    }

    public static List<Pair<ColumnMetaData.Rep, Integer>> fieldClasses(final RelProtoDataType protoRowType,
                                                                       final JavaTypeFactory typeFactory) {
        final RelDataType rowType = protoRowType.apply(typeFactory);
        return Lists.transform(rowType.getFieldList(), f -> {
            final RelDataType type = f.getType();
            final Class clazz = (Class) typeFactory.getJavaClass(type);
            final ColumnMetaData.Rep rep =
                    Util.first(ColumnMetaData.Rep.of(clazz),
                            ColumnMetaData.Rep.OBJECT);
            return Pair.of(rep, type.getSqlTypeName().getJdbcOrdinal());
        });
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
}
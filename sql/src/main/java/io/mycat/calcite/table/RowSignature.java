package io.mycat.calcite.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import javafx.util.Pair;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import sun.util.resources.cldr.fr.CalendarData_fr_CI;

import javax.sql.RowSetInternal;
import java.nio.charset.Charset;
import java.sql.JDBCType;
import java.util.List;
import java.util.Map;

public class RowSignature {
    private final Map<String, JDBCType> columnTypes;
    private final List<String> columnNames;

    private RowSignature(final List<Pair<String, JDBCType>> columnTypeList) throws Exception {
        final Map<String, JDBCType> columnTypes0 = Maps.newHashMap();
        final ImmutableList.Builder<String> columnNamesBuilder =
                ImmutableList.builder();

        int i = 0;
        for (Pair<String, JDBCType> pair : columnTypeList) {
            final JDBCType existingType = columnTypes0.get(pair.getKey());
            if (existingType != null && existingType != pair.getValue()) {
                // throw new exception
                throw new Exception("aa");
            }

            columnTypes0.put(pair.getKey(), pair.getValue());
            columnNamesBuilder.add(pair.getKey());

        }
        this.columnTypes = ImmutableMap.copyOf(columnTypes0);
        this.columnNames = columnNamesBuilder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public JDBCType getColumnType(final String name) {
        return columnTypes.get(name);
    }

    public List<String>getRowOrder() {
        return columnNames;
    }

    public RelDataType getRelDataType(final RelDataTypeFactory factory) throws Exception {
        final RelDataTypeFactory.FieldInfoBuilder builder = factory.builder();
        for (final String columnName : columnNames) {
            final JDBCType columnType = getColumnType(columnName);
            final RelDataType type;

            switch (columnType) {
                case VARCHAR:
                    type = factory.createTypeWithCharsetAndCollation(
                            factory.createSqlType(SqlTypeName.VARCHAR),
                            Charset.defaultCharset(),
                            SqlCollation.IMPLICIT);
                    break;
                case INTEGER:
                    type = factory.createSqlType(SqlTypeName.INTEGER);
                    break;
                case BIGINT:
                    type = factory.createSqlType(SqlTypeName.BIGINT);
                    break;
                case DATE:
                    type = factory.createSqlType(SqlTypeName.DATE);
                    break;
                case DECIMAL:
                    type = factory.createSqlType(SqlTypeName.DECIMAL);
                    break;
                case LONGVARBINARY:
                    type = factory.createSqlType(SqlTypeName.VARBINARY);
                    break;
                case DOUBLE:
                    type = factory.createSqlType(SqlTypeName.DOUBLE);
                    break;
                default:
                    throw new Exception("");
            }

            builder.add(columnName, type);
        }

        return builder.build();
    }

    public static class Builder {
        private final List<Pair<String, JDBCType>> columnTypeList;

        private Builder() {
            this.columnTypeList = Lists.newArrayList();
        }

        public Builder add(String columnName, JDBCType columnType) {
            columnTypeList.add(new Pair<>(columnName, columnType));
            return this;
        }

        public RowSignature build() throws Exception {
            return new RowSignature(columnTypeList);
        }
    }
}

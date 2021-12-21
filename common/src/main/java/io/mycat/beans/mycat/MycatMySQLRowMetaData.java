package io.mycat.beans.mycat;

import com.mysql.cj.CharsetMapping;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.vertx.mysqlclient.impl.datatype.DataType;
import lombok.Getter;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
public class MycatMySQLRowMetaData implements MycatRowMetaData {
    final List<ColumnDefPacket> columnDefPackets;

    public MycatMySQLRowMetaData(List<ColumnDefPacket> columnDefPackets) {
        this.columnDefPackets = columnDefPackets;
    }

    @Override
    public int getColumnCount() {
        return columnDefPackets.size();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return false;
    }

    @Override
    public boolean isSigned(int column) {
        int columnFlags = columnDefPackets.get(column).getColumnFlags();
        boolean unsigned = (columnFlags & MySQLFieldsType.UNSIGNED_FLAG) != 0;
        return !unsigned;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return columnDefPackets.get(column).getColumnLength();
    }

    @Override
    public String getColumnName(int column) {
        return columnDefPackets.get(column).getColumnNameString();
    }

    @Override
    public String getSchemaName(int column) {
        return new String(columnDefPackets.get(column).getColumnSchema());
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return columnDefPackets.get(column).getColumnDecimals();
    }

    @Override
    public String getTableName(int column) {
        return new String(columnDefPackets.get(column).getColumnTable());
    }

    @Override
    public int getColumnType(int column) {
        ColumnDefPacket columnDefPacket = columnDefPackets.get(column);
        return DataType.valueOf(columnDefPacket.getColumnType()).jdbcType.getVendorTypeNumber();
    }

    @Override
    public String getColumnLabel(int column) {
        return columnDefPackets.get(column).getColumnNameString();
    }

    @Override
    public ResultSetMetaData metaData() {
        return null;
    }

    @Override
    public boolean isNullable(int column) {
        return (columnDefPackets.get(column).getColumnFlags() & MySQLFieldsType.NOT_NULL_FLAG) == 0;
    }

    @Override
    public String toString() {
        return columnDefPackets.stream().map(i -> i.toString()).collect(Collectors.joining(","));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MycatMySQLRowMetaData that = (MycatMySQLRowMetaData) o;
        return Objects.equals(columnDefPackets, that.columnDefPackets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnDefPackets);
    }

    @Override
    public MycatRelDataType getMycatRelDataType() {
        List<MycatField> mycatFields = columnDefPackets.stream().map(new Function<ColumnDefPacket, MycatField>() {
            @Override
            public MycatField apply(ColumnDefPacket columnDefPacket) {
                DataType dataType = DataType.valueOf(columnDefPacket.getColumnType());
                String columnName = new String(columnDefPacket.getColumnName());
                JDBCType jdbcType = dataType.jdbcType;
                boolean nullable = columnDefPacket.isNullable();
                boolean signed = columnDefPacket.isSigned();
                int columnDecimals = columnDefPacket.getColumnDecimals();
                MycatDataType mycatDataType = MycatDataType.VARCHAR;
                boolean text = columnDefPacket.getColumnCharsetSet()!= CharsetMapping.MYSQL_COLLATION_INDEX_binary;
                boolean binary = (columnDefPacket.getColumnFlags() & MySQLFieldsType.BINARY_FLAG) != 0;
                boolean blob = (columnDefPacket.getColumnFlags() & MySQLFieldsType.BLOB_FLAG) != 0;
                switch (dataType) {
                    case INT1:
                        if (columnName.toLowerCase().contains("bool")) {
                            mycatDataType = MycatDataType.BOOLEAN;
                        } else {
                            mycatDataType = signed ? MycatDataType.TINYINT : MycatDataType.UNSIGNED_TINYINT;
                        }
                        break;
                    case INT2:
                        mycatDataType = signed ? MycatDataType.SHORT : MycatDataType.UNSIGNED_SHORT;
                        break;
                    case INT3:
                    case INT4:
                        mycatDataType = signed ? MycatDataType.INT : MycatDataType.UNSIGNED_INT;
                        break;
                    case INT8:
                        mycatDataType = signed ? MycatDataType.LONG : MycatDataType.UNSIGNED_LONG;
                        break;
                    case DOUBLE:
                        mycatDataType = MycatDataType.DOUBLE;
                        break;
                    case FLOAT:
                        mycatDataType = MycatDataType.FLOAT;
                        break;
                    case NUMERIC:
                        mycatDataType = MycatDataType.DECIMAL;
                        break;
                    case STRING:
                        if (blob || binary) {
                            mycatDataType = MycatDataType.BINARY;
                        } else {
                            mycatDataType = MycatDataType.CHAR;
                        }
                        break;
                    case VARSTRING:
                        if (blob || binary) {
                            mycatDataType = MycatDataType.BINARY;
                        } else {
                            mycatDataType = MycatDataType.VARCHAR;
                        }
                        break;
                    case TINYBLOB:
                    case BLOB:
                    case MEDIUMBLOB:
                    case LONGBLOB:
                        if (text){
                            mycatDataType = MycatDataType.VARCHAR;
                        }else {
                            mycatDataType = MycatDataType.BINARY;
                        }
                        break;
                    case DATE:
                        mycatDataType = MycatDataType.DATE;
                        break;
                    case TIME:
                        mycatDataType = MycatDataType.TIME;
                        break;
                    case TIMESTAMP:
                    case DATETIME:
                        mycatDataType = MycatDataType.DATETIME;
                        break;
                    case YEAR:
                        mycatDataType = MycatDataType.YEAR;
                        break;
                    case BIT:
                        if (columnDefPacket.getColumnLength() == 1) {
                            mycatDataType = MycatDataType.BOOLEAN;
                        } else {
                            mycatDataType = MycatDataType.BIT;
                        }
                        break;
                    case JSON:
                    case GEOMETRY:
                        mycatDataType = MycatDataType.VARCHAR;
                        break;
                    case NULL:
                        mycatDataType = MycatDataType.NULL;
                        break;
                    case UNBIND:
                        throw new UnsupportedOperationException();
                }

                return MycatField.of(columnName, mycatDataType, nullable, columnDecimals, 0);
            }
        }).collect(Collectors.toList());
        return MycatRelDataType.of(mycatFields);
    }
}

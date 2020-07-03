package io.mycat.resultset;

import com.mysql.cj.MysqlType;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.util.ByteUtil;

import java.sql.Types;
import java.util.Iterator;

public class BinaryResultSetResponse extends AbstractMycatResultSetResponse {
    MysqlType[] types;

    public BinaryResultSetResponse(RowBaseIterator iterator) {
        super(iterator);
        final MycatRowMetaData mycatRowMetaData = iterator.getMetaData();
        int columnCount = mycatRowMetaData.getColumnCount();
        for (int i = 1, j = 0; i <= columnCount; i++, j++) {
            types[j] = MysqlType.getByJdbcType(mycatRowMetaData.getColumnType(i));
        }
    }

    @Override
    public Iterator rowIterator() {
        final RowBaseIterator rowBaseIterator = iterator;
        return new Iterator<byte[]>() {

            @Override
            public boolean hasNext() {
                return rowBaseIterator.next();
            }

            @Override
            public byte[] next() {
                byte[][] rows = new byte[types.length][];
                for (int i = 1, j = 0; i <= types.length; i++, j++) {
                    MysqlType columnType = types[j];
                    Object object = rowBaseIterator.getObject(i);
                    byte[] value;
                    switch (columnType) {
                        case TINYINT:
                        case TINYINT_UNSIGNED:
                        case BOOLEAN:
                            value = convertToByte(object);
                            break;
                        case SMALLINT:
                        case SMALLINT_UNSIGNED:
                            value = convertToInt16((Number) object);
                            break;
                        case INT:
                        case INT_UNSIGNED:
                            value = convertToInt32((Number) object);
                            break;
                        case FLOAT:
                        case FLOAT_UNSIGNED:
                            value = convertToFloat32((Number) object);
                            break;
                        case DOUBLE_UNSIGNED:
                        case DOUBLE:
                            value = convertToFloat64((Number) object);
                            break;
                        case NULL:
                            value = null;
                            break;
                        case TIMESTAMP:
                            break;
                        case BIGINT_UNSIGNED:
                        case BIGINT:
                            value = convertToInt64((Number) object);
                            break;
                        case MEDIUMINT:
                            break;
                        case MEDIUMINT_UNSIGNED:
                            break;
                        case DATE:
                            break;
                        case TIME:
                            break;
                        case DATETIME:
                            break;
                        case YEAR:
                            value = convertToInt16((Number) object);
                            break;
                        case DECIMAL:
                        case DECIMAL_UNSIGNED:
                        case VARCHAR:
                        case VARBINARY:
                        case BIT:
                        case JSON:
                        case ENUM:
                        case SET:
                        case TINYBLOB:
                        case TINYTEXT:
                        case MEDIUMBLOB:
                        case MEDIUMTEXT:
                        case LONGBLOB:
                        case LONGTEXT:
                        case BLOB:
                        case TEXT:
                        case CHAR:
                        case BINARY:
                        case GEOMETRY:
                        case UNKNOWN:
                            value = convertString(object);
                            break;
                    }
                    Object object = iterator.getObject(i);
                    byte[] value;
                    if (iterator.wasNull()) {
                        value = null;
                    } else {
                        switch (columnType) {
                            case Types.BIT: {

                                break;
                            }
                            case Types.TINYINT:
                            case Types.SMALLINT:
                            case Types.INTEGER: {
                                value = convertInt((Number) object);
                                break;
                            }
                            case Types.BIGINT: {
                                value = convertLong((Number) object);
                                break;
                            }
                            case Types.FLOAT: {

                            }
                            case Types.REAL: {

                            }

                            case Types.DOUBLE: {

                            }
                            case Types.NUMERIC: {

                            }
                            case Types.DECIMAL: {

                            }
                            case Types.CHAR: {

                            }
                            case Types.VARCHAR: {

                            }
                            case Types.LONGVARCHAR: {

                            }
                            case Types.DATE: {

                            }

                            case Types.TIME: {

                            }
                            case Types.TIMESTAMP: {

                            }
                            case Types.BINARY: {

                            }
                            case Types.VARBINARY: {

                            }
                            case Types.LONGVARBINARY: {

                            }

                            case Types.NULL: {

                            }
                            case Types.OTHER: {

                            }
                            case Types.JAVA_OBJECT: {

                            }
                            case Types.DISTINCT: {

                            }
                            case Types.STRUCT: {

                            }

                            case Types.ARRAY: {

                            }

                            case Types.BLOB: {

                            }

                            case Types.CLOB: {

                            }
                            case Types.REF: {

                            }
                            case Types.DATALINK: {

                            }
                            case Types.BOOLEAN: {

                            }
                            case Types.ROWID: {

                            }
                            case Types.NCHAR: {

                            }

                            case Types.NVARCHAR: {

                            }

                            case Types.LONGNVARCHAR: {

                            }

                            case Types.NCLOB: {

                            }
                            case Types.SQLXML: {

                            }
                            case Types.REF_CURSOR: {

                            }
                            case Types.TIME_WITH_TIMEZONE: {

                            }
                            case Types.TIMESTAMP_WITH_TIMEZONE: {

                            }

                            break;
                            default:
                                throw new IllegalStateException("Unexpected value: " + columnType);
                        }
                    }

                }
                return new byte[0];
            }
        };
    }

    private byte[] convertToFloat32(Number object) {
        return ByteUtil.getBytes(object.floatValue());
    }

    private byte[] convertToInt64(Number object) {
        return ByteUtil.getBytes(object.longValue());
    }

    private byte[] convertString(Object object) {
        if (object instanceof byte[]){
            return (byte[])object;
        }
        return object.toString().getBytes();
    }

    private byte[] convertToFloat64(Number object) {
        return ByteUtil.getBytes(object.doubleValue());
    }

    private byte[] convertToInt32(Number object) {
        return ByteUtil.getBytes(object.intValue());
    }

    private byte[] convertToInt16(Number object) {
        return ByteUtil.getBytes(object.shortValue());
    }

    private byte[] convertLong(Number object) {
        return ByteUtil.getBytes(object.longValue());
    }

    private byte[] convertInt(Number object) {
        return ByteUtil.getBytes(object.intValue());
    }

    private byte[] convertToByte(Object object) {
        if (object == Boolean.TRUE) {
            return ByteUtil.getBytes(1);
        } else if (object == Boolean.FALSE) {
            return ByteUtil.getBytes(0);
        } else if (object instanceof Number) {
            byte b = ((Number) object).byteValue();
            return ByteUtil.getBytes(b);
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.vertx;

import io.mycat.MySQLPacketUtil;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.resultset.BinaryResultSetResponse;
import io.mycat.resultset.TextConvertorImpl;
import org.apache.calcite.avatica.util.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;
import java.util.function.Function;

public class ResultSetMapping {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResultSetMapping.class);

    public static Function<Object[], byte[]> concertToDirectTextResultSet(MycatRowMetaData rowMetaData) {
        int columnCount = rowMetaData.getColumnCount();
        return new Function<Object[], byte[]>() {
            @Override
            public byte[] apply(Object[] objects) {
                byte[][] row = new byte[columnCount][];
                for (int columnIndex = 0, rowIndex = 0; rowIndex < columnCount; columnIndex++, rowIndex++) {

                    int columnType = rowMetaData.getColumnType(columnIndex);
                    switch (columnType) {
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                        case 2004://blob
                        case Types.BINARY: {
                            Object o = objects[columnIndex];
                            byte[] bytes = null;
                            if (o == null) {

                            } else if (o instanceof byte[]) {
                                bytes = (byte[]) o;
                            } else if (o instanceof ByteString) {
                                bytes = ((ByteString) o).getBytes();
                            }
                            row[rowIndex] = bytes == null ? null : bytes;
                            break;
                        }
                        case Types.TIME: {
                            Object o = objects[columnIndex];
                            if (o == null) {
                                row[rowIndex] = null;
                            } else if (o instanceof Duration) {
                                row[rowIndex] = TextConvertorImpl.getBytes((Duration) o);
                            } else if (o instanceof LocalTime) {
                                row[rowIndex] = TextConvertorImpl.getBytes((LocalTime) o);
                            } else if (o instanceof Time) {
                                row[rowIndex] = TextConvertorImpl.getBytes(((Time) o).toLocalTime());
                            } else if (o instanceof String) {
                                row[rowIndex] = ((String) o).getBytes();
                            } else {
                                LOGGER.error(" unsupport type:{}  value:{}", o.getClass(), o);
                                throw new UnsupportedOperationException();
                            }
                            break;
                        }
                        case Types.TIMESTAMP_WITH_TIMEZONE:
                        case Types.TIMESTAMP: {
                            Object o = objects[columnIndex];
                            if (o == null) {
                                row[rowIndex] = null;
                            } else if (o instanceof Timestamp) {
                                row[rowIndex] = TextConvertorImpl.getBytes(((Timestamp) o).toLocalDateTime());
                            } else if (o instanceof LocalDateTime) {
                                row[rowIndex] = TextConvertorImpl.getBytes((LocalDateTime) o);
                            } else {
                                LOGGER.error(" unsupport type:{}  value:{}", o.getClass(), o);
                                throw new UnsupportedOperationException();
                            }
                            break;
                        }
                        case Types.BIT: {
                            Object o = objects[columnIndex];
                            if (o == null) {
                                row[rowIndex] = null;
                            } else if (o instanceof Boolean) {
                                row[rowIndex] = ((Boolean) o).booleanValue() ? new byte[]{1} : new byte[]{0};
                            } else if (o instanceof Number) {
                                row[rowIndex] = new byte[]{((Number) o).byteValue()};
                            } else {
                                LOGGER.error(" unsupport type:{}  value:{}", o.getClass(), o);
                                throw new UnsupportedOperationException();
                            }
                            break;
                        }
                        case Types.BOOLEAN: {
                            Object o = objects[columnIndex];
                            if (o == null) {
                                row[rowIndex] = null;
                            } else if (o instanceof Boolean) {
                                row[rowIndex] = TextConvertorImpl.INSTANCE.convertBoolean((Boolean) o);
                            } else if (o instanceof Number) {
                                long l = ((Number) o).longValue();
                                if ((l == -1 || l > 0)) {
                                    row[rowIndex] = TextConvertorImpl.ONE;
                                } else {
                                    row[rowIndex] = TextConvertorImpl.ZERO;
                                }
                            } else if (o instanceof String) {
                                row[rowIndex] = ((String) o).getBytes();
                            } else {
                                LOGGER.error(" unsupport type:{}  value:{}", o.getClass(), o);
                                throw new UnsupportedOperationException();
                            }
                            break;
                        }
                        case Types.REAL:
                        case Types.FLOAT:
                        case Types.DOUBLE: {
                            Object o = objects[columnIndex];
                            if (o == null) {
                                row[rowIndex] = null;
                            } else {
                                if (o instanceof Float) {

                                    Float aFloat = (Float) o;
                                    if (aFloat.longValue() == aFloat.floatValue()) {
                                        row[rowIndex] = Objects.toString(aFloat.longValue()).getBytes();
                                    } else {
                                        row[rowIndex] = Objects.toString(aFloat).getBytes();
                                    }
                                } else if (o instanceof Double) {
                                    Double aDouble = (Double) o;
                                    if (aDouble.longValue() == aDouble.doubleValue()) {
                                        row[rowIndex] = Objects.toString(aDouble.longValue()).getBytes();
                                    } else {
                                        row[rowIndex] = Objects.toString(aDouble).getBytes();
                                    }
                                } else {
                                    row[rowIndex] = Objects.toString(o).getBytes();
                                }
                            }
                            break;
                        }
                        default:
                            Object object = objects[columnIndex];
                            row[rowIndex] = (object == null ? null : Objects.toString(object).getBytes());
                            break;
                    }

                }
                return MySQLPacketUtil.generateTextRow(row);
            }
        };
    }

    public static Function<Object[], byte[]> concertToDirectBinaryResultSet(MycatRowMetaData rowMetaData) {
        return objects -> {
            byte[][] bytes = new byte[objects.length][];
            for (int i = 0; i < objects.length; i++) {
                if (objects[i] == null) {
                    bytes[i] = null;
                } else {
                    bytes[i] = BinaryResultSetResponse.getBytes(rowMetaData.getColumnType(i), objects[i]);
                }
            }
            return MySQLPacketUtil.generateBinaryRow(bytes);
        };
    }
}

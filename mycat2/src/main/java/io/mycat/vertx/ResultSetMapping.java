package io.mycat.vertx;

import io.mycat.MySQLPacketUtil;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.resultset.BinaryResultSetResponse;
import io.mycat.resultset.TextConvertorImpl;
import io.mycat.resultset.TextResultSetResponse;
import kotlin.jvm.functions.Function1;
import org.apache.calcite.avatica.util.ByteString;

import java.sql.Types;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;
import java.util.function.Function;

public class ResultSetMapping {
    public static Function<Object[],byte[]> concertToDirectTextResultSet(MycatRowMetaData rowMetaData){
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
                            if (o == null){
                                row[rowIndex] = null;
                            }else if (o instanceof Duration){
                                row[rowIndex] = TextConvertorImpl.getBytes((Duration) o);
                            }else if (o instanceof LocalTime){
                                row[rowIndex] = TextConvertorImpl.getBytes((LocalTime) o);
                            }else {
                                throw new UnsupportedOperationException();
                            }
                            break;
                        }
                        case Types.TIMESTAMP_WITH_TIMEZONE:
                        case Types.TIMESTAMP: {
                            Object o = objects[columnIndex];
                            if (o == null){
                                row[rowIndex] = null;
                            }else if (o instanceof LocalDateTime){
                                row[rowIndex] = TextConvertorImpl.getBytes((LocalDateTime) o);
                            }
                            break;
                        }
                        default:
                            Object object = objects[columnIndex];
                            row[rowIndex] =(object==null?null: Objects.toString(object).getBytes());
                            break;
                    }

                }
                return MySQLPacketUtil.generateTextRow(row);
            }
        };
    }
    public static Function<Object[],byte[]> concertToDirectBinaryResultSet(MycatRowMetaData rowMetaData){
        return objects -> {
            byte[][] bytes = new byte[objects.length][];
            for (int i = 0; i < objects.length; i++) {
                if (objects[i]==null){
                    bytes[i]=null;
                }else {
                    bytes[i]=BinaryResultSetResponse.getBytes(rowMetaData.getColumnType(i),objects[i]);
                }
            }
            return MySQLPacketUtil.generateBinaryRow(bytes);
        };
    }
}

package io.mycat;

import io.mycat.beans.mycat.MycatRowMetaData;
import kotlin.jvm.functions.Function1;

import java.sql.Types;
import java.util.Objects;
import java.util.function.Function;

public class ResultSetMapping {
    public static Function<Object[],byte[]> concertToDirectResultSet(MycatRowMetaData rowMetaData){
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
                        case Types.BINARY:
                            byte[] bytes = (byte[]) objects[columnIndex];
                            row[rowIndex] = bytes ==null? null : bytes;
                            break;
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
}

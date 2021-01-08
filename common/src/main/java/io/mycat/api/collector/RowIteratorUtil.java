package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;

import java.util.Objects;

public class RowIteratorUtil {
    public static String dumpColumnInfo(RowBaseIterator iterator) {
        StringBuilder sb = new StringBuilder();
        MycatRowMetaData mycatRowMetaData = iterator.getMetaData();
        int columnCount = mycatRowMetaData.getColumnCount();

        while (iterator.next()) {
            for (int i = 0; i < columnCount; i++) {
                sb.append(mycatRowMetaData.getColumnName(i)).append(":").append(Objects.toString(iterator.getObject(i))).append(" | ");
            }
            sb.append("\n");
        }

        return sb.toString();
    }
}
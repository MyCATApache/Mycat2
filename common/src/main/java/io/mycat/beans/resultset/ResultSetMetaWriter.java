package io.mycat.beans.resultset;

import io.mycat.beans.mycat.MycatRowMetaData;
import org.apache.arrow.vector.types.pojo.ArrowType;

public interface ResultSetMetaWriter {
    public void addName(String name);

    public void addNullable(boolean nullable);

    public void addType(ArrowType from);

    public  void startColumn();

    public  void endColumn();

    MycatRowMetaData build();
}

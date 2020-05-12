package io.mycat.calcite.table;

import io.mycat.Identical;
import io.mycat.beans.mycat.MycatRowMetaData;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

public abstract class PreComputationSQLTable extends AbstractTable implements ScannableTable, Identical {
    volatile Enumerable<Object[]> enumerable;

    public abstract String getTargetName();

    public abstract String getSql();

    public abstract List<Object> params();

    public void setEnumerable(Enumerable<Object[]> enumerable) {
        this.enumerable = enumerable;
    }

    public abstract MycatRowMetaData getMetaData();
}
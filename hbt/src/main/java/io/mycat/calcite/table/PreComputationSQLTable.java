package io.mycat.calcite.table;

import io.mycat.Identical;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

public abstract class PreComputationSQLTable extends AbstractTable implements ScannableTable, Identical {
    public abstract String getTargetName();

    public abstract String getSql();

    public abstract List<Object> params();
}
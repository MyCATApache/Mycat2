package io.mycat.calcite.logic;

import io.mycat.calcite.Identical;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

public abstract class PreComputationSQLTable extends AbstractTable implements ScannableTable , Identical {
    public abstract String getTargetName();
}
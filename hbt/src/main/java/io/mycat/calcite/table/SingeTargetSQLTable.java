package io.mycat.calcite.table;

import java.util.List;

public abstract class SingeTargetSQLTable extends EnumerableTable{
    public abstract String getTargetName();

    public abstract String getSql();

    public abstract List<Object> params();
}
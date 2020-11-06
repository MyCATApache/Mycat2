package io.mycat.calcite.table;

import io.mycat.Identical;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

public abstract class EnumerableTable extends AbstractTable implements ScannableTable, Identical {
    volatile Enumerable<Object[]> enumerable;
    public void setEnumerable(Enumerable<Object[]> enumerable) {
        this.enumerable = enumerable;
    }

    public MycatRowMetaData getMetaData() {
        return new CalciteRowMetaData(getRowType(MycatCalciteSupport.INSTANCE.TypeFactory).getFieldList());
    }
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return enumerable;
    }

    public boolean existsEnumerable(){
        return enumerable!=null;
    }

}
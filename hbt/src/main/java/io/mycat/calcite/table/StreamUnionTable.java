package io.mycat.calcite.table;

import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.table.EnumerableTable;
import io.mycat.calcite.table.MycatSQLTableScan;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;
import java.util.stream.Collectors;

public class StreamUnionTable extends EnumerableTable {
    private List<MycatSQLTableScan> enumerableTables;

    public StreamUnionTable(List<MycatSQLTableScan> enumerableTables) {
        this.enumerableTables = enumerableTables;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return enumerableTables.get(0).getRowType(MycatCalciteSupport.INSTANCE.TypeFactory);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.concat(enumerableTables.stream().map(i->i.scan(root)).collect(Collectors.toList()));
    }

    @Override
    public boolean existsEnumerable() {
        return true;
    }
}
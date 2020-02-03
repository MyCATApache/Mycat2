package io.mycat.calcite.relBuilder;

import io.mycat.QueryBackendTask;
import io.mycat.calcite.CalciteUtls;
import io.mycat.calcite.MyCatResultSetEnumerable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.impl.AbstractTable;


/**
 * chenjunwen
 */
public class MycatTransientSQLTable extends AbstractTable
        implements TransientTable, ScannableTable {
    private String targetName;
    private final String name;
    private final RelDataType protoRowType;
    private RelNode input;
    private String sqlSelect;

    public MycatTransientSQLTable(String targetName, String name, RelNode input, String sqlSelect) {
        this.targetName = targetName;
        this.name = name;
        this.protoRowType = input.getRowType();
        this.input = input;
        this.sqlSelect = sqlSelect;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        // add the table into the schema, so that it is accessible by any potential operator
        root.getRootSchema().add(name, this);
        return new MyCatResultSetEnumerable(CalciteUtls.getCancelFlag(root), new QueryBackendTask(sqlSelect, targetName));
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.copyType(protoRowType);
    }

}
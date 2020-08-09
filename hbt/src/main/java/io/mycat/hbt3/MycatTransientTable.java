package io.mycat.hbt3;

import io.mycat.DataNode;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

public class MycatTransientTable extends AbstractTable implements TransientTable {
    private final DataNode dataNode;
    private final RelDataType type;

    protected MycatTransientTable(DataNode dataNode, RelDataType type) {
        this.dataNode = dataNode;
        this.type = type;
    }

    public static MycatTransientTable create(DataNode dataNode, RelDataType type) {
        return new MycatTransientTable(dataNode,type);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return type;
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        return false;
    }
}
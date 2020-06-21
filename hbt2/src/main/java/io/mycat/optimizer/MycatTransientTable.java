package io.mycat.optimizer;

import io.mycat.DataNode;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Getter
public class MycatTransientTable implements TransientTable {
    private final RelNode relNode;
    private final List<DataNode> dataNodes;

    public static MycatTransientTable create(List<DataNode> dataNodes, RelNode relNode) {
        return new MycatTransientTable(dataNodes, relNode);
    }

    public MycatTransientTable(List<DataNode> dataNodes, RelNode relNode) {
        this.relNode = relNode;
        this.dataNodes = dataNodes;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return relNode.getRowType();
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
package io.mycat.calcite.logic;

import io.mycat.calcite.CalciteConvertors;
import io.mycat.calcite.MetadataManager;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

public abstract class MycatTableBase extends AbstractTable implements ProjectableFilterableTable {


    public abstract MetadataManager.LogicTable logicTable();

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return CalciteConvertors.getRelDataType(logicTable().getRawColumns(), typeFactory);
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
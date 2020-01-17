package io.mycat.calcite.logic;

import io.mycat.QueryBackendTask;
import io.mycat.calcite.CalciteConvertors;
import io.mycat.calcite.CalciteUtls;
import io.mycat.calcite.MetadataManager;
import io.mycat.calcite.MyCatResultSetEnumerable;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

import static io.mycat.calcite.CalciteUtls.getQueryBackendTasks;

public  interface MycatTableBase extends TranslatableTable, ProjectableFilterableTable {

        public MetadataManager.LogicTable logicTable();

        @Override
        default public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return CalciteConvertors.getRelDataType(logicTable().getRawColumns(), typeFactory);
        }

        @Override
        default public Statistic getStatistic() {
            return Statistics.UNKNOWN;
        }

        @Override
        default Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        @Override
        default boolean isRolledUp(String column) {
            return false;
        }

        @Override
        default boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
            return false;
        }

        @Override
        default public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
            List<QueryBackendTask> backendTasks = getQueryBackendTasks(logicTable(),  filters, projects);
            return new MyCatResultSetEnumerable(CalciteUtls.getCancelFlag(root), backendTasks);
        }

        @Override
        default public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
            return LogicalTableScan.create(context.getCluster(), relOptTable);
        }
    }
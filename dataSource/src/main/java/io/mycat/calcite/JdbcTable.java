/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite;

import io.mycat.QueryBackendTask;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.mycat.calcite.CalciteUtls.getQueryBackendTasks;
import static io.mycat.calcite.CalciteUtls.getRelDataType;

/**
 * @author Junwen Chen
 * @author Weiqing Xu
 **/
public class JdbcTable implements TranslatableTable, ProjectableFilterableTable {
    private MetadataManager.LogicTable table;
    private final static Logger LOGGER = LoggerFactory.getLogger(JdbcTable.class);

    public JdbcTable(MetadataManager.LogicTable table) {
        this.table = table;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        return LogicalTableScan.create(toRelContext.getCluster(), relOptTable);
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return getRelDataType(table.getRawColumns(),typeFactory);
    }


    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    /**
     * @param column
     * @return default false
     */
    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    /**
     * @param column
     * @param call
     * @param parent
     * @param config
     * @return default false
     */
    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        return false;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, final int[] projects) {
        List<QueryBackendTask> backendTasks = getQueryBackendTasks(this.table,  filters, projects);
        return new MyCatResultSetEnumerable(CalciteUtls.getCancelFlag(root), backendTasks);
    }


    public MetadataManager.LogicTable getTable() {
        return table;
    }

}
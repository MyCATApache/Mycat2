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
package io.mycat.calcite.table;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;
import io.mycat.metadata.GlobalTableHandler;
import io.mycat.metadata.NormalTableHandler;
import io.mycat.router.ShardingTableHandler;
import io.mycat.statistic.StatisticCenter;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatLogicTable extends MycatTableBase {
    final TableHandler table;
    final Statistic statistic;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatLogicTable.class);

    public MycatLogicTable(TableHandler t) {
        this.table = t;
        statistic =Statistics.createStatistic(table.getSchemaName(),table.getTableName(),table.getColumns());
    }

    @Override
    public TableHandler logicTable() {
        return table;
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }

}
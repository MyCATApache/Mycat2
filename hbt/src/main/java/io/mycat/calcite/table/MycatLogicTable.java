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

import io.mycat.LogicTableType;
import io.mycat.TableHandler;
import io.mycat.calcite.rewriter.Distribution;
import lombok.Getter;
import org.apache.calcite.schema.Statistic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatLogicTable extends MycatTableBase implements AbstractMycatTable {
    final TableHandler table;
    final Statistic statistic;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatLogicTable.class);

    public MycatLogicTable(TableHandler t) {
        this.table = t;
        this.statistic = Statistics.createStatistic(table.getSchemaName(), table.getTableName(), table.getColumns());
    }

    @Override
    public TableHandler logicTable() {
        return table;
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }

    public Distribution createDistribution() {
        switch (table.getType()) {
            case SHARDING:
                ShardingTable shardingTableHandler = (ShardingTable) this.table;
                return Distribution.of(shardingTableHandler);
            case GLOBAL:
                GlobalTable globalTableHandler = (GlobalTable) this.table;
                return Distribution.of(globalTableHandler);
            case NORMAL:
                NormalTable normalTable = (NormalTable) this.table;
                return Distribution.of(normalTable);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSharding() {
        return table.getType()== LogicTableType.SHARDING;
    }

    @Override
    public boolean isNormal() {
        return  table.getType()== LogicTableType.NORMAL;
    }

    @Override
    public boolean isCustom() {
        return table.getType() ==LogicTableType.CUSTOM;
    }

    @Override
    public boolean isBroadCast() {
        return table.getType() ==LogicTableType.GLOBAL;
    }
}
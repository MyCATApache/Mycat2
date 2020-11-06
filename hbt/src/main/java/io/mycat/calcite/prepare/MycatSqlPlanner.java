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
package io.mycat.calcite.prepare;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.CalciteRunners;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.table.PreComputationSQLTable;
import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.PlanRunner;
import io.mycat.util.Explains;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public class MycatSqlPlanner implements PlanRunner {
    private final List<PreComputationSQLTable> preComputationSQLTables;
    private final RelNode relNode;
    private final MycatCalciteSQLPrepareObject prepare;
    private final MycatCalciteDataContext mycatCalciteDataContext;
    private final Supplier<RowBaseIterator> runner;

    @SneakyThrows
    public MycatSqlPlanner(MycatCalciteSQLPrepareObject prepare, String sql, MycatDBContext uponDBContext) {
        this.prepare = prepare;
        this.mycatCalciteDataContext = MycatCalciteSupport.INSTANCE.create(uponDBContext);
        MycatCalcitePlanner planner = MycatCalciteSupport.INSTANCE.createPlanner(mycatCalciteDataContext);
        this.relNode = CalciteRunners.complie(planner, sql);
        this.preComputationSQLTables = planner.preComputeSeq(this.relNode);
        this.runner = CalciteRunners.run(this.mycatCalciteDataContext, preComputationSQLTables, relNode);
    }

    public List<String> explain() {
        String collect = preComputationSQLTables.stream()
                .map(preComputationSQLTable ->
                        new Explains.PrepareCompute(preComputationSQLTable.getTargetName(), preComputationSQLTable.getSql(), preComputationSQLTable.params()).toString()).collect(Collectors.joining(",\n"));


        return Explains.explain(prepare.getSql(),
                collect,
                MycatCalciteSupport.INSTANCE.convertToHBTText(relNode, mycatCalciteDataContext),
                MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(this.relNode, mycatCalciteDataContext));
    }

    @Override
    public RowBaseIterator run() {
        return runner.get();
    }
}
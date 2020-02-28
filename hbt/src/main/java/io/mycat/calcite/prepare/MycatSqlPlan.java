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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.MycatCalciteContext;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalcitePlanner;
import io.mycat.calcite.logic.MycatTransientSQLTable;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * @author Junwen Chen
 **/
public class MycatSqlPlan implements PlanRunner {
    private final List<MycatTransientSQLTable> tableScans;
    private final RelNode relNode;
    static final Cache<String, RelNode> cache = CacheBuilder.newBuilder().maximumSize(65535).build();
    private final MycatCalcitePrepare prepare;

    @SneakyThrows
    public MycatSqlPlan(MycatCalcitePrepare prepare, String sql) {
        this.prepare = prepare;
        MycatCalcitePlanner planner = MycatCalciteContext.INSTANCE.createPlanner(prepare.getDefaultSchemaName());
        this.relNode = cache.get(prepare.getDefaultSchemaName() + ":" + sql, new Callable<RelNode>() {
            @Override
            public RelNode call() throws Exception {
                return CalciteRunners.complie(planner, sql);
            }
        });
        this.tableScans = planner.collectMycatTransientSQLTableScan(this.relNode);
    }


    @SneakyThrows
    public Supplier<RowBaseIterator> run(MycatCalciteDataContext dataContext) {
        return CalciteRunners.run(dataContext, relNode);
    }

    public List<String> explain() {
        return new ExpainObject(prepare.getSql(),
                MycatCalciteContext.INSTANCE.convertToHBTText(relNode),
                MycatCalciteContext.INSTANCE.convertToMycatRelNodeText(this.relNode))
                .explain();
    }


}
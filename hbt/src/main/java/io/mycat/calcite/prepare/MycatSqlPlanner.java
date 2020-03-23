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
import io.mycat.upondb.ProxyInfo;
import io.mycat.util.Explains;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Junwen Chen
 **/
public class MycatSqlPlanner implements PlanRunner {
    private final RelNode relNode;
    private final MycatCalciteSQLPrepareObject prepare;
    private final MycatCalciteDataContext mycatCalciteDataContext;
    private final DatasourceInfo datasourceInfo;


    @SneakyThrows
    public MycatSqlPlanner(MycatCalciteSQLPrepareObject prepare, String sql, MycatDBContext uponDBContext) {
        this.prepare = prepare;
        this.mycatCalciteDataContext = MycatCalciteSupport.INSTANCE.create(uponDBContext);
        MycatCalcitePlanner planner = MycatCalciteSupport.INSTANCE.createPlanner(mycatCalciteDataContext);
        this.relNode = CalciteRunners.complie(planner, sql, prepare.isForUpdate());
        this.datasourceInfo = planner.preComputeSeq(this.relNode);
    }

    public List<String> explain() {
        RelDataType rowType = relNode.getRowType();
        return Explains.explain(prepare.getSql(),
                MycatCalciteSupport.INSTANCE.convertToHBTText(datasourceInfo.preSeq),
                MycatCalciteSupport.INSTANCE.dumpMetaData(rowType),
                MycatCalciteSupport.INSTANCE.convertToHBTText(relNode, mycatCalciteDataContext),
                MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(this.relNode, mycatCalciteDataContext));
    }


    @Override
    public RowBaseIterator run() {
        Supplier<RowBaseIterator> runner = CalciteRunners.run(this.mycatCalciteDataContext, datasourceInfo.preSeq, relNode);
        ;
        return runner.get();
    }

    public ProxyInfo tryGetProxyInfo() {
        DatasourceInfo ds = this.datasourceInfo;
        List<PreComputationSQLTable> preSeq = ds.getPreSeq();
        Map<String, List<PreComputationSQLTable>> map = ds.getMap();
        if (preSeq.isEmpty() && map.size() == 1) {
            Map.Entry<String, List<PreComputationSQLTable>> next = map.entrySet().iterator().next();
            String key = next.getKey();
            if (next.getValue().size() == 1) {
                PreComputationSQLTable preComputationSQLTable = next.getValue().get(0);
                return new ProxyInfo(preComputationSQLTable.getTargetName(), preComputationSQLTable.getSql(), prepare.isForUpdate());
            }

        }
        return null;
    }

}
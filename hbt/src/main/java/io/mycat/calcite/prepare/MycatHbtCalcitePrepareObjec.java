package io.mycat.calcite.prepare;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CalciteRowMetaData;
import io.mycat.calcite.MycatCalciteContext;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalcitePlanner;
import io.mycat.hbt.HBTConvertor;
import io.mycat.hbt.ast.base.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class MycatHbtCalcitePrepareObjec extends MycatHbtPrepareObject {
    final Schema schema ;
    public MycatHbtCalcitePrepareObjec(Long id, int paramCount, Schema schema) {
        super(id, paramCount);
        this.schema = schema;
    }
    @Override
    public MycatRowMetaData resultSetRowType() {
        RelNode handle = getRelNode(Collections.emptyList());
        return new CalciteRowMetaData(handle.getRowType().getFieldList());
    }

    private RelNode getRelNode(List<Object> params) {
        MycatCalcitePlanner planner1 = MycatCalciteContext.INSTANCE.createPlanner(null);
        HBTConvertor hbtConvertor = new HBTConvertor(planner1.createRelBuilder(), params);
        return hbtConvertor.handle(schema);
    }

    @Override
    public PlanRunner plan(List<Object> params) {
        RelNode relNode = getRelNode(params);
        return new PlanRunner() {
            @Override
            public Supplier<RowBaseIterator> run(MycatCalciteDataContext dataContext) {
                return CalciteRunners.run(dataContext, relNode);
            }

            @Override
            public List<String> explain() {
                return ExpainObject.explain(MycatCalciteContext.INSTANCE.convertToSql(relNode, MysqlSqlDialect.DEFAULT),
                        MycatCalciteContext.INSTANCE.convertToHBTText(relNode),
                        MycatCalciteContext.INSTANCE.convertToMycatRelNodeText(relNode));
            }
        };
    }
}
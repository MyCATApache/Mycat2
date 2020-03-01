package io.mycat.calcite.prepare;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CalciteRowMetaData;
import io.mycat.calcite.MycatCalciteContext;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalcitePlanner;
import io.mycat.calcite.logic.PreComputationSQLTable;
import io.mycat.hbt.HBTConvertor;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.upondb.UponDBContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class MycatHbtCalcitePrepareObject extends MycatHbtPrepareObject {
    final Schema schema;
    private final MycatCalciteDataContext mycatCalciteDataContext;
    private List<PreComputationSQLTable> preComputationSQLTables;

    public MycatHbtCalcitePrepareObject(Long id, int paramCount, Schema schema, UponDBContext uponDBContext) {
        super(id, paramCount);
        this.schema = schema;
        this.mycatCalciteDataContext = MycatCalciteContext.INSTANCE.create(uponDBContext);
    }

    @Override
    public MycatRowMetaData resultSetRowType() {
        RelNode handle = getRelNode(Collections.emptyList());
        return new CalciteRowMetaData(handle.getRowType().getFieldList());
    }

    @Override
    public PlanRunner plan(List<Object> params) {
        RelNode relNode = getRelNode(params);
        Supplier<RowBaseIterator> run = CalciteRunners.run(mycatCalciteDataContext, preComputationSQLTables, relNode);
        return new PlanRunner() {
            @Override
            public RowBaseIterator run() {
                return run.get();
            }

            @Override
            public List<String> explain() {
                return ExpainObject.explain(MycatCalciteContext.INSTANCE.convertToSql(relNode, MysqlSqlDialect.DEFAULT),
                        MycatCalciteContext.INSTANCE.convertToHBTText(relNode, mycatCalciteDataContext),
                        MycatCalciteContext.INSTANCE.convertToMycatRelNodeText(relNode, mycatCalciteDataContext));
            }
        };
    }

    private RelNode getRelNode(List<Object> params) {
        MycatCalcitePlanner planner1 = MycatCalciteContext.INSTANCE.createPlanner(mycatCalciteDataContext);
        HBTConvertor hbtConvertor = new HBTConvertor( params,mycatCalciteDataContext);

        RelNode handle = hbtConvertor.handle(schema);
        this.preComputationSQLTables = planner1.preComputeSeq(handle);
        return handle;
    }

}
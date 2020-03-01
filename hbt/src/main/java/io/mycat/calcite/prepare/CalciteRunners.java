package io.mycat.calcite.prepare;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.CalciteConvertors;
import io.mycat.calcite.EnumeratorRowIterator;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalcitePlanner;
import io.mycat.calcite.logic.PreComputationSQLTable;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import lombok.SneakyThrows;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelRunners;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.function.Supplier;

import static io.mycat.calcite.MycatCalcitePlanner.toPhysical;

public class CalciteRunners {

    @SneakyThrows
    public static RelNode complie(MycatCalcitePlanner planner, String sql) {
        SqlNode sqlNode = planner.parse(sql);
        SqlNode validate = planner.validate(sqlNode);
        RelNode relNode = planner.convert(validate);
        return complie(planner, relNode);
    }

    public static RelNode complie(MycatCalcitePlanner planner, RelNode relNode) {
        RelNode relNode1 = planner.eliminateLogicTable(relNode);
        relNode = planner.pushDownBySQL(relNode1);

        RelNode phy = toPhysical(relNode, relOptPlanner -> {
            RelOptUtil.registerDefaultRules(relOptPlanner, false, false);
        });
        //修复变成物理表达式后无法运行,所以重新编译成逻辑表达式
        return new ToLogicalConverter(planner.createRelBuilder(relNode.getCluster())).visit(phy);
    }

    @SneakyThrows
    public static Supplier<RowBaseIterator> run(MycatCalciteDataContext dataContext, List<PreComputationSQLTable> preComputationSQLTables, RelNode relNode) {
        try {
            MycatRowMetaData mycatRowMetaData = CalciteConvertors.getMycatRowMetaData(relNode.getRowType());
            System.out.println(RelOptUtil.toString(relNode));
            ArrayBindable bindable1 = Interpreters.bindable(relNode);
            Enumerator<Object[]> enumerator = bindable1.bind(dataContext).enumerator();
            return () -> {
                for (PreComputationSQLTable preComputationSQLTable : preComputationSQLTables) {
                    dataContext.preComputation(preComputationSQLTable);
                }
                return new EnumeratorRowIterator(mycatRowMetaData, enumerator);
            };
        } catch (java.lang.AssertionError | Exception e) {//实在运行不了使用原来的方法运行
            e.printStackTrace();
            System.err.println(e);
            PreparedStatement run = RelRunners.run(relNode);
            return new Supplier<RowBaseIterator>() {

                @SneakyThrows
                @Override
                public RowBaseIterator get() {
                    return new JdbcRowBaseIteratorImpl(run, run.executeQuery());
                }
            };
        }
    }


}
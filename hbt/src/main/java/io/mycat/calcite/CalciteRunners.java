package io.mycat.calcite;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.prepare.MycatCalcitePlanner;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.calcite.table.PreComputationSQLTable;
import lombok.SneakyThrows;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Supplier;

import static io.mycat.calcite.prepare.MycatCalcitePlanner.toPhysical;

public class CalciteRunners {
    private final static Logger LOGGER = LoggerFactory.getLogger(CalciteRunners.class);

    @SneakyThrows
    public static RelNode complie(MycatCalcitePlanner planner, String sql, boolean forUpdate) {
        SqlNode sqlNode = planner.parse(sql);
        SqlNode validate = planner.validate(sqlNode);
        RelNode relNode = planner.convert(validate);
        return complie(planner, relNode, forUpdate);
    }

    public static RelNode complie(MycatCalcitePlanner planner, RelNode relNode, boolean forUpdate) {
        RelNode relNode1 = planner.eliminateLogicTable(relNode);
//        relNode = planner.pushDownBySQL(relNode1, forUpdate);
//
//        RelNode phy = toPhysical(relNode, relOptPlanner -> {
//            RelOptUtil.registerDefaultRules(relOptPlanner, false, false);
//        });
        //修复变成物理表达式后无法运行,所以重新编译成逻辑表达式
        return new ToLogicalConverter(planner.createRelBuilder(relNode.getCluster())).visit(relNode1);
    }

    @SneakyThrows
    public static Supplier<RowBaseIterator> run(MycatCalciteDataContext dataContext, List<PreComputationSQLTable> preComputationSQLTables, RelNode relNode) {
        try {
            MycatRowMetaData mycatRowMetaData = CalciteConvertors.getMycatRowMetaData(relNode.getRowType());
            ArrayBindable bindable1 = Interpreters.bindable(relNode);
            Enumerator<Object[]> enumerator = bindable1.bind(dataContext).enumerator();
            return () -> {
                if (preComputationSQLTables != null) {
                    for (PreComputationSQLTable preComputationSQLTable : preComputationSQLTables) {
                        dataContext.preComputation(preComputationSQLTable);
                    }
                }
                return new EnumeratorRowIterator(mycatRowMetaData, enumerator);
            };
        } catch (Throwable e) {
            e.printStackTrace();
            LOGGER.info("该关系表达式不被支持的" + relNode);
            LOGGER.error("", e);
            PreparedStatement run = RelRunners.run(relNode);
            return new Supplier<RowBaseIterator>() {
                @Override
                @SneakyThrows
                public RowBaseIterator get() {
                    return new JdbcRowBaseIterator(null, run, run.executeQuery(), new Closeable() {
                        @Override
                        public void close() throws IOException {
                            try {
                                run.close();
                            } catch (SQLException e1) {
                                LOGGER.error("", e1);
                            }
                        }
                    }, null);
                }
            };
        }
    }
}
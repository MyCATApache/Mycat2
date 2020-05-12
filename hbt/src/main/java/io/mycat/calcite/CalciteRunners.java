package io.mycat.calcite;

import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.prepare.MycatCalcitePlanner;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.calcite.table.PreComputationSQLTable;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.upondb.MycatDBContext;
import lombok.SneakyThrows;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
        relNode = planner.pushDownBySQL(relNode1, forUpdate);

        RelNode phy = toPhysical(relNode, relOptPlanner -> {
            RelOptUtil.registerDefaultRules(relOptPlanner, false, false);
        });
        //修复变成物理表达式后无法运行,所以重新编译成逻辑表达式
        return new ToLogicalConverter(planner.createRelBuilder(relNode.getCluster())).visit(phy);
    }

    @SneakyThrows
    public static RowBaseIterator run(MycatCalciteDataContext calciteDataContext, RelNode relNode) {
            MycatRowMetaData mycatRowMetaData = CalciteConvertors.getMycatRowMetaData(relNode.getRowType());
            Map<String, List<PreComputationSQLTable>> map = new HashMap<>();
            relNode.accept(new RelShuttleImpl() {
                @Override
                public RelNode visit(TableScan scan) {
                    PreComputationSQLTable unwrap = scan.getTable().unwrap(PreComputationSQLTable.class);
                    if (unwrap != null) {
                        List<PreComputationSQLTable> tables = map.computeIfAbsent(unwrap.getTargetName(), s -> new ArrayList<>(2));
                        tables.add(unwrap);
                    }
                    return super.visit(scan);
                }
            });
            MycatDBContext context = calciteDataContext.getUponDBContext();
            MycatDataContext dataContext = context.unwrap(MycatDataContext.class);
            AtomicBoolean cancelFlag = context.cancelFlag();
            ExecutorService parallelExecutor = JdbcRuntime.INSTANCE.getParallelExecutor();

            if (context.isInTransaction()) {
                TransactionSession transactionSession = dataContext.getTransactionSession();
                for (Map.Entry<String, List<PreComputationSQLTable>> entry : map.entrySet()) {
                    String k = entry.getKey();
                    List<PreComputationSQLTable> list = entry.getValue();
                    DefaultConnection connection = transactionSession.getConnection(k);
                    if (list.size() > 1) {
                        throw new IllegalAccessException("该执行计划重复拉取同一个数据源的数据");
                    }
                    PreComputationSQLTable table = list.get(0);
                    Future<RowBaseIterator> submit = parallelExecutor
                            .submit(() -> connection.executeQuery(table.getMetaData(), table.getSql(), true));
                    table.setEnumerable(new AbstractEnumerable<Object[]>() {
                        @Override
                        @SneakyThrows
                        public Enumerator<Object[]> enumerator() {
                            return new MyCatResultSetEnumerator(cancelFlag, submit.get(1, TimeUnit.MINUTES));
                        }
                    });
                }

            } else {
                for (Map.Entry<String, List<PreComputationSQLTable>> entry : map.entrySet()) {
                    for (PreComputationSQLTable v : entry.getValue()) {
                        DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(v.getTargetName());
                        Future<RowBaseIterator> submit = parallelExecutor
                                .submit(() -> connection.executeQuery(v.getMetaData(), v.getSql(), true));
                        v.setEnumerable(new AbstractEnumerable<Object[]>() {
                            @Override
                            @SneakyThrows
                            public Enumerator<Object[]> enumerator() {
                                return new MyCatResultSetEnumerator(cancelFlag, submit.get(1, TimeUnit.MINUTES));
                            }
                        });
                    }
                }
            }
            ArrayBindable bindable1 = Interpreters.bindable(relNode);

            Enumerable<Object[]> bind = bindable1.bind(calciteDataContext);
            Enumerator<Object[]> enumerator = bind.enumerator();
            return new EnumeratorRowIterator(mycatRowMetaData, enumerator);
        }
    }
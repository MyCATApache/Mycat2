package io.mycat.calcite;

import io.mycat.MycatConnection;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.prepare.MycatCalcitePlanner;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.calcite.rules.StreamUnionRule;
import io.mycat.calcite.table.SingeTargetSQLTable;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.sqlRecorder.SqlRecorderRuntime;
import io.mycat.sqlRecorder.SqlRecorderType;
import io.mycat.upondb.MycatDBContext;
import lombok.SneakyThrows;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CalciteRunners {
    private final static Logger LOGGER = LoggerFactory.getLogger(CalciteRunners.class);

    @SneakyThrows
    public static RelNode compile(MycatCalcitePlanner planner, String sql, boolean forUpdate) {
        long start = System.currentTimeMillis();
        SqlRecorderRuntime.INSTANCE.start();
        SqlRecorderRuntime.INSTANCE.addRecord(SqlRecorderType.AT_START, sql, start);
        SqlNode sqlNode = planner.parse(sql);
        SqlNode validate = planner.validate(sqlNode);
        RelNode relNode = planner.convert(validate);
        long cro = System.currentTimeMillis();
        SqlRecorderRuntime.INSTANCE.addRecord(SqlRecorderType.COMPILE_SQL, sql, cro - start);
        try {
            return compile(planner, relNode, forUpdate);
        } finally {
            SqlRecorderRuntime.INSTANCE.addRecord(SqlRecorderType.RBO, sql, System.currentTimeMillis() - cro);
        }
    }

    public static RelNode compile(MycatCalcitePlanner planner, RelNode relNode, boolean forUpdate) {
        try {
            relNode = Objects.requireNonNull(planner.eliminateLogicTable(relNode));
            try {
                /**
                 * 上拉union仅仅是优化不应该导致关系表达式不能执行
                 */
                relNode = Objects.requireNonNull(planner.pullUpUnion(relNode));
            } catch (Throwable e) {
                LOGGER.error("", e);
            }
            relNode = Objects.requireNonNull(planner.pushDownBySQL(relNode, forUpdate));
            return Objects.requireNonNull(relNode);
        } catch (Throwable e) {
            LOGGER.error("", e);
            throw e;
        }
    }


    @SneakyThrows
    public static RowBaseIterator run(String sql, MycatCalciteDataContext calciteDataContext, RelNode relNode) {
        Map<String, List<SingeTargetSQLTable>> map = new HashMap<>();
        relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                SingeTargetSQLTable unwrap = scan.getTable().unwrap(SingeTargetSQLTable.class);
                if (unwrap != null && !unwrap.existsEnumerable()) {
                    List<SingeTargetSQLTable> tables = map.computeIfAbsent(unwrap.getTargetName(), s -> new ArrayList<>(2));
                    tables.add(unwrap);
                }
                return super.visit(scan);
            }
        });

        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        hepProgramBuilder.addMatchLimit(64);

        hepProgramBuilder.addRuleInstance(StreamUnionRule.INSTANCE);
        final HepPlanner planner2 = new HepPlanner(hepProgramBuilder.build());
        planner2.setRoot(relNode);
        relNode = planner2.findBestExp();

        //check
        relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalUnion union) {
                if (union.getInputs().size() > 2) {
                    throw new AssertionError("union input more 2");
                }
                return super.visit(union);
            }
        });
        long startGetConnectionTime = System.currentTimeMillis();
        fork(sql,calciteDataContext, map);
        long cbo = System.currentTimeMillis();
        SqlRecorderRuntime.INSTANCE.addRecord(SqlRecorderType.GET_CONNECTION, sql, cbo - startGetConnectionTime);
        ArrayBindable bindable1 = Interpreters.bindable(relNode);
        SqlRecorderRuntime.INSTANCE.addRecord(SqlRecorderType.CBO, sql, System.currentTimeMillis() - cbo);

        Enumerable<Object[]> bind = bindable1.bind(calciteDataContext);


        Enumerator<Object[]> enumerator = bind.enumerator();

        return new EnumeratorRowIterator(CalciteConvertors.getMycatRowMetaData(relNode.getRowType()), enumerator,
                () -> SqlRecorderRuntime.INSTANCE.addRecord(SqlRecorderType.AT_END, sql, System.currentTimeMillis()));
    }

    private static void fork(String sql, MycatCalciteDataContext calciteDataContext, Map<String, List<SingeTargetSQLTable>> map) throws IllegalAccessException {
        MycatDBContext uponDBContext = calciteDataContext.getUponDBContext();
        AtomicBoolean cancelFlag = uponDBContext.cancelFlag();
        if (uponDBContext.isInTransaction()) {
            for (Map.Entry<String, List<SingeTargetSQLTable>> entry : map.entrySet()) {
                String datasource = entry.getKey();
                List<SingeTargetSQLTable> list = entry.getValue();
                SingeTargetSQLTable table = list.get(0);
                if (table.existsEnumerable()) {
                    continue;
                }
                MycatConnection connection = uponDBContext.getConnection(datasource);
                if (list.size() > 1) {
                    throw new IllegalAccessException("事务内该执行计划重复拉取同一个数据源的数据");
                }
                Future<RowBaseIterator> submit = JdbcRuntime.INSTANCE.getFetchDataExecutorService()
                        .submit(() -> {
                            long start = System.currentTimeMillis();
                            try {
                                return connection.executeQuery(table.getMetaData(), table.getSql());
                            } finally {
                                SqlRecorderRuntime.INSTANCE.addRecord(SqlRecorderType.CONNECTION_QUERY_RESPONSE,
                                        sql, System.currentTimeMillis() - start);
                            }
                        });
                table.setEnumerable(new AbstractEnumerable<Object[]>() {
                    @Override
                    @SneakyThrows
                    public Enumerator<Object[]> enumerator() {
                        return new MyCatResultSetEnumerator(cancelFlag, submit.get(1, TimeUnit.MINUTES));
                    }
                });
            }
        } else {
            Iterator<String> iterator = map.entrySet().stream()
                    .flatMap(i -> i.getValue().stream())
                    .filter(i -> !i.existsEnumerable())
                    .map(i -> i.getTargetName()).iterator();
            Map<String, Deque<MycatConnection>> nameMap = JdbcRuntime.INSTANCE.getConnection(iterator);
            for (Map.Entry<String, List<SingeTargetSQLTable>> entry : map.entrySet()) {
                List<SingeTargetSQLTable> value = entry.getValue();
                for (SingeTargetSQLTable v : value) {
                    MycatConnection connection = nameMap.get(v.getTargetName()).remove();
                    uponDBContext.addCloseResource(connection);
                    Future<RowBaseIterator> submit = JdbcRuntime.INSTANCE.getFetchDataExecutorService()
                            .submit(() -> {
                                long start = System.currentTimeMillis();
                                try {
                                    return connection.executeQuery(v.getMetaData(), v.getSql());
                                } finally {
                                    SqlRecorderRuntime.INSTANCE.addRecord(SqlRecorderType.CONNECTION_QUERY_RESPONSE,
                                            sql, System.currentTimeMillis() - start);
                                }
                            });
                    AbstractEnumerable enumerable = new AbstractEnumerable<Object[]>() {
                        @Override
                        @SneakyThrows
                        public Enumerator<Object[]> enumerator() {
                            LOGGER.info("拉取数据" + v.getTargetName() + " sql:" + v.getSql());
                            return new MyCatResultSetEnumerator(cancelFlag, submit.get());
                        }
                    };
                    v.setEnumerable(enumerable);
                }
            }
        }
    }

    @SneakyThrows
    public static RelNode compile(MycatCalcitePlanner planner, SqlNode sql, boolean forUpdate) {
        SqlNode validate = planner.validate(sql);
        RelNode relNode = planner.convert(validate);
        return compile(planner, relNode, forUpdate);
    }
}
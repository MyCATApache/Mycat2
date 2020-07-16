package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.MycatConnection;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.prepare.MycatCalcitePlanner;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.calcite.table.SingeTargetSQLTable;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExecutorImplementorImpl;
import io.mycat.hbt4.MycatRel;
import io.mycat.sqlrecorder.SqlRecorder;
import io.mycat.sqlrecorder.SqlRecorderRuntime;
import io.mycat.sqlrecorder.SqlRecorderType;
import io.mycat.upondb.MycatDBContext;
import io.mycat.util.TimeProvider;
import lombok.SneakyThrows;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
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
    public static RelNode compile(MycatCalcitePlanner planner, String sql, SqlNode sqlNode , boolean forUpdate) {
        long start = TimeProvider.INSTANCE.now();
        SqlRecorder recorder = SqlRecorderRuntime.INSTANCE.getCurrentRecorder();
        recorder.start();
        recorder.addRecord(SqlRecorderType.AT_START, sql, start);
        planner.parse();
        SqlNode validate = planner.validate(sqlNode);
        RelNode relNode = planner.convert(validate);
        long cro = TimeProvider.INSTANCE.now();
        recorder.addRecord(SqlRecorderType.COMPILE_SQL, sql, cro - start);
        try {
            return compile(planner, relNode, forUpdate);
        } finally {
            recorder.addRecord(SqlRecorderType.RBO, sql, TimeProvider.INSTANCE.now() - cro);
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
        SqlRecorder recorder = SqlRecorderRuntime.INSTANCE.getCurrentRecorder();
        Map<String, List<SingeTargetSQLTable>> map = new HashMap<>();

        relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                SingeTargetSQLTable unwrap = scan.getTable().unwrap(SingeTargetSQLTable.class);
                if (unwrap != null && !unwrap.existsEnumerable()) {
                    String targetName = calciteDataContext.getUponDBContext().resolveFinalTargetName(unwrap.getTargetName());
                    List<SingeTargetSQLTable> tables = map.computeIfAbsent(targetName, s -> new ArrayList<>(2));
                    tables.add(unwrap);
                }
                return super.visit(scan);
            }
        });
       MycatRel mycatRel = (MycatRel)DrdsRunner.optimizeWithCBO(relNode);
        long startGetConnectionTime = TimeProvider.INSTANCE.now();
        fork(sql, calciteDataContext, map);
        long cbo = TimeProvider.INSTANCE.now();
        recorder.addRecord(SqlRecorderType.GET_CONNECTION, sql, cbo - startGetConnectionTime);
        ExecutorImplementorImpl executorImplementor = new ExecutorImplementorImpl(ImmutableList.of(),null);
        Executor executor = mycatRel.implement(executorImplementor);
        long execution_start = TimeProvider.INSTANCE.now();
        recorder.addRecord(SqlRecorderType.CBO, sql, execution_start - cbo);
        executor.open();
        Enumerator<Object[]> enumerator = Linq4j.iterableEnumerator( ()-> executor.outputBbjectIterator());

        return new EnumeratorRowIterator(CalciteConvertors.getMycatRowMetaData(relNode.getRowType()), enumerator,
                () -> {
                    recorder.addRecord(SqlRecorderType.EXECUTION_TIME, sql, TimeProvider.INSTANCE.now()-execution_start);
                    recorder.addRecord(SqlRecorderType.AT_END, sql, TimeProvider.INSTANCE.now());
                });
    }

    private static void fork(String sql, MycatCalciteDataContext calciteDataContext, Map<String, List<SingeTargetSQLTable>> map) throws IllegalAccessException {
        MycatDBContext uponDBContext = calciteDataContext.getUponDBContext();
        AtomicBoolean cancelFlag = uponDBContext.cancelFlag();
        SqlRecorder recorder = SqlRecorderRuntime.INSTANCE.getCurrentRecorder();

        if (uponDBContext.isInTransaction()) {
            for (Map.Entry<String, List<SingeTargetSQLTable>> entry : map.entrySet()) {
                String datasource = entry.getKey();
                List<SingeTargetSQLTable> list = entry.getValue();
                SingeTargetSQLTable table = list.get(0);
                if (table.existsEnumerable()) {
                    continue;
                }
                MycatConnection connection = uponDBContext.getConnection(datasource);
                long start = System.currentTimeMillis();
                if (list.size() > 1) {
                    throw new IllegalAccessException("事务内该执行计划重复拉取同一个数据源的数据");
                }
                Future<RowBaseIterator> submit = JdbcRuntime.INSTANCE.getFetchDataExecutorService()
                        .submit(() -> {
                            try {
                                return connection.executeQuery(table.getMetaData(), table.getSql());
                            } finally {
                                recorder.addRecord(SqlRecorderType.CONNECTION_QUERY_RESPONSE,
                                        sql, TimeProvider.INSTANCE.now()- start);
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
            long start = System.currentTimeMillis();
            for (Map.Entry<String, List<SingeTargetSQLTable>> entry : map.entrySet()) {
                List<SingeTargetSQLTable> value = entry.getValue();
                for (SingeTargetSQLTable v : value) {
                    MycatConnection connection = nameMap.get(v.getTargetName()).remove();
                    uponDBContext.addCloseResource(connection);
                    Future<RowBaseIterator> submit = JdbcRuntime.INSTANCE.getFetchDataExecutorService()
                            .submit(() -> {
                                try {
                                    return connection.executeQuery(v.getMetaData(), v.getSql());
                                } finally {
                                    recorder.addRecord(SqlRecorderType.CONNECTION_QUERY_RESPONSE,
                                            sql, TimeProvider.INSTANCE.now() - start);
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
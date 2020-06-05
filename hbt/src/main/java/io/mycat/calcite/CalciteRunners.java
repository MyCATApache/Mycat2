package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.MycatConnection;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.prepare.MycatCalcitePlanner;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.calcite.table.MycatSQLTableScan;
import io.mycat.calcite.table.SingeTargetSQLTable;
import io.mycat.calcite.table.StreamUnionTable;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.upondb.MycatDBContext;
import lombok.SneakyThrows;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
        relNode = planner.eliminateLogicTable(relNode);
        StringWriter stringWriter = new StringWriter();
//        final RelWriterImpl pw =
//                new RelWriterImpl(new PrintWriter(stringWriter));
//        relNode.explain(pw);
//        LOGGER.debug(stringWriter.toString());
        relNode = planner.pullUpUnion(relNode);
        relNode = planner.pushDownBySQL(relNode, forUpdate);
        return relNode;
    }


    @SneakyThrows
    public static RowBaseIterator run(MycatCalciteDataContext calciteDataContext, RelNode relNode) {
        fork(calciteDataContext, relNode);
        ArrayBindable bindable1 = Interpreters.bindable(relNode);
        Enumerable<Object[]> bind = bindable1.bind(calciteDataContext);

        Enumerator<Object[]> enumerator = bind.enumerator();
        return new EnumeratorRowIterator(CalciteConvertors.getMycatRowMetaData(relNode.getRowType()), enumerator);
    }

    private static void fork(MycatCalciteDataContext calciteDataContext, RelNode relNode) throws IllegalAccessException {
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

        relNode = relNode.accept(new RelShuttleImpl() {

            @Override
            public RelNode visit(LogicalUnion union) {
                boolean inTransaction = calciteDataContext.getUponDBContext().isInTransaction();
                if (union.getInputs().size() > 0) {
                    RelBuilder relBuilder = MycatCalciteSupport.INSTANCE.relBuilderFactory.create(union.getCluster(), null);
                    if (union.getInputs().stream().allMatch(p->  p.getTable()!=null&&p.getTable().unwrap(MycatSQLTableScan.class)!=null)) {
                        List<MycatSQLTableScan> relNodes = (List)
                                (union.getInputs().stream().map(p -> p.getTable().unwrap(MycatSQLTableScan.class)).collect(Collectors.toList()));
                        StreamUnionTable scanOperator = new StreamUnionTable(relNodes);
                        RelOptTable table = RelOptTableImpl.create(
                                null,
                                scanOperator.getRowType(MycatCalciteSupport.INSTANCE.TypeFactory),
                                scanOperator,
                                ImmutableList.of(union.toString()));
                        return LogicalTableScan.create(union.getCluster(), table, ImmutableList.of());
                    }

                    //calcite的默认union执行器的输入不能超过2个
                    if (union.getInputs().size() > 2) {
                        return union.getInputs().stream().reduce((relNode1, relNode2) -> {
                            relBuilder.push(relNode1);
                            relBuilder.push(relNode2);
                            return relBuilder.union(!union.isDistinct()).build();
                        }).orElse(union);
                    }

                }
                return union;
            }
        });

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
                    throw new IllegalAccessException("该执行计划重复拉取同一个数据源的数据");
                }
                Future<RowBaseIterator> submit = JdbcRuntime.INSTANCE.getFetchDataExecutorService()
                        .submit(() -> connection.executeQuery(table.getMetaData(), table.getSql()));
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
                            .submit(() -> connection.executeQuery(v.getMetaData(), v.getSql()));
                    AbstractEnumerable enumerable = new AbstractEnumerable<Object[]>() {
                        @Override
                        @SneakyThrows
                        public Enumerator<Object[]> enumerator() {
                            LOGGER.info("------!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                            return new MyCatResultSetEnumerator(cancelFlag, submit.get());
                        }
                    };
                    v.setEnumerable(enumerable);
                }
            }
        }
    }
}
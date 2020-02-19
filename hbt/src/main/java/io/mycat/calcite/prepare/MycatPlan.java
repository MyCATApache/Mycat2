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
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.*;
import io.mycat.calcite.logic.MycatTransientSQLTable;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import lombok.SneakyThrows;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.tools.ValidationException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static io.mycat.calcite.MycatCalcitePlanner.toPhysical;

/**
 * @author Junwen Chen
 **/
public class MycatPlan {
    private String defaultSchemaName;
    private final String sql;
    private final List<MycatTransientSQLTable> tableScans;
    private final RelNode relNode;
    static final Cache<String, RelNode> cache = CacheBuilder.newBuilder().maximumSize(65535).build();

    @SneakyThrows
    public MycatPlan(String defaultSchemaName, String sql) throws ValidationException, RelConversionException, SqlParseException {
        this.sql = sql;
        this.defaultSchemaName = defaultSchemaName;
        MycatCalcitePlanner planner = MycatCalciteContext.INSTANCE.createPlanner(defaultSchemaName);

        this.relNode = cache.get(defaultSchemaName + ":" + sql, () -> complie(planner, defaultSchemaName, sql));
        this.tableScans = planner.collectMycatTransientSQLTableScan(this.relNode);
    }

    @SneakyThrows
    private static RelNode complie(MycatCalcitePlanner planner, String defaultSchemaName, String sql) throws SqlParseException, ValidationException, RelConversionException {
        SqlNode sqlNode = planner.parse(sql);
        SqlNode validate = planner.validate(sqlNode);
        RelNode relNode = planner.convert(validate);
        RelNode relNode1 = planner.eliminateLogicTable(relNode);
        relNode = planner.pushDownBySQL(relNode1);

        RelNode phy = toPhysical(relNode, relOptPlanner -> {
            RelOptUtil.registerDefaultRules(relOptPlanner, false, false);
        });
        //修复变成物理表达式后无法运行,所以重新编译成逻辑表达式
        return new ToLogicalConverter(planner.createRelBuilder(relNode.getCluster())).visit(phy);
    }

    @SneakyThrows
    public Supplier<RowBaseIterator> run(MycatCalciteDataContext dataContext) {
        try {
            MycatRowMetaData mycatRowMetaData = CalciteConvertors.getMycatRowMetaData(relNode.getRowType());
            System.out.println(RelOptUtil.toString(relNode));
            ArrayBindable bindable1 = Interpreters.bindable(relNode);
            Enumerator<Object[]> enumerator = bindable1.bind(dataContext).enumerator();
            return () -> new EnumeratorRowIterator(mycatRowMetaData, enumerator);
        } catch (java.lang.AssertionError | Exception e) {//实在运行不了使用原来的方法运行
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

    public List<String> explain() {
        SqlExplainLevel expplanAttributes = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
        final StringWriter sw = new StringWriter();
        final RelWriter planWriter =
                new RelWriterImpl(
                        new PrintWriter(sw), expplanAttributes, false);

        MycatCalcitePlanner planner = MycatCalciteContext.INSTANCE.createPlanner(defaultSchemaName);
        relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatTransientSQLTable unwrap = scan.getTable().unwrap(MycatTransientSQLTable.class);
                if (unwrap != null) {
                    return unwrap.toRel(ViewExpanders.toRelContext(planner, planner.newCluster()), scan.getTable());
                }
                return super.visit(scan);
            }
        }).explain(planWriter);
        String message = sw.toString();
        return new ArrayList<>(Arrays.asList(message.split("\n")));
    }
}
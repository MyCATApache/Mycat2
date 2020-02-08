/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.function.Consumer;
import java.util.function.Supplier;

public enum CalciteEnvironment {
    INSTANCE;
    final Logger LOGGER = LoggerFactory.getLogger(CalciteEnvironment.class);



    private CalciteEnvironment() {
          }

    public TextResultSetResponse getConnection(String defaultSchema, String sql) throws Exception {
        MycatCalcitePlanner planner = MycatCalciteContext.INSTANCE.createPlanner(defaultSchema);
        SqlNode sqlNode = planner.parse(sql);
        RelNode relNode = planner.convert( planner.validate(sqlNode));
        RelNode relNode1 = planner.eliminateLogicTable(relNode);
        RelNode relNode2 = planner.pushDownBySQL(relNode1);
        Supplier<RowBaseIterator> run = planner.run(relNode2);
        return new TextResultSetResponse(run.get());
    }



    public CalciteConnection getRawConnection() {
        try {
            Connection connection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false;lex=MYSQL;fun=mysql;conformance=MYSQL_5");
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            return calciteConnection;
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new RuntimeException(e);
        }
    }

    public static RelNode toPhysical(RelNode rel, Consumer<RelOptPlanner> setting) {
        final RelOptPlanner planner = rel.getCluster().getPlanner();
        planner.clear();
        setting.accept(planner);
        final Program program = Programs.of(RuleSets.ofList(planner.getRules()));
        return program.run(planner, rel, rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
                ImmutableList.of(), ImmutableList.of());
    }
}
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

import io.mycat.BackendTableInfo;
import io.mycat.calcite.logic.MycatLogicTable;
import io.mycat.calcite.logic.MycatPhysicalTable;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum CalciteEnvironment {
    INSTANCE;
    final Logger LOGGER = LoggerFactory.getLogger(CalciteEnvironment.class);
    final static SqlParser.Config SQL_PARSER_CONFIG = SqlParser.configBuilder().setLex(Lex.MYSQL)
            .setConformance(SqlConformanceEnum.MYSQL_5)
            .setCaseSensitive(false).build();
    volatile SchemaPlus ROOT_SCHEMA;

    private CalciteEnvironment() {
        Driver driver = new Driver();//触发驱动注册
        final String charset = "UTF-8";
        System.setProperty("saffron.default.charset", charset);
        System.setProperty("saffron.default.nationalcharset", charset);
        System.setProperty("calcite.default.charset", charset);
        System.setProperty("saffron.default.collat​​ion.tableName", charset + "$ en_US");

        flash();
    }

    public TextResultSetResponse getConnection(String defaultSchema, String sql) throws Exception {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        if (defaultSchema != null) {
            configBuilder.defaultSchema(ROOT_SCHEMA.getSubSchema(defaultSchema));
        }
        configBuilder.parserConfig(SQL_PARSER_CONFIG);
        FrameworkConfig config = configBuilder.build();
        PlannerImpl planner = new PlannerImpl(config);
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        RelNode convert = planner.convert(validate);

        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        hepProgramBuilder
                .addRuleInstance(FilterTableScanRule.INSTANCE)
                .addRuleInstance(ProjectTableScanRule.INSTANCE);
        hepProgramBuilder.addRuleInstance(PushDownFilter.PROJECT_ON_FILTER2);
        final HepPlanner planner2 = new HepPlanner(hepProgramBuilder.build());

        planner2.setRoot(convert);
        RelNode bestExp = planner2.findBestExp();
        planner2.setRoot(bestExp);
        bestExp = planner2.findBestExp();
        System.out.println(RelOptUtil.toString(bestExp));
//        scan.childrenAccept(new RelVisitor() {
//            @Override
//            public void visit(RelNode node, int ordinal, RelNode parent) {
//                if (node instanceof Bindables.BindableTableScan) {
//                    parent.replaceInput(ordinal,);
//                    tableScans.add((TableScan) node);
//                }
//                if (node instanceof LogicalTableScan) {
//                    tableScans.add((TableScan) node);
//                }
//                super.visit(node, ordinal, parent);
//            }
//        });
        CalciteDataContext dataContext = new CalciteDataContext(ROOT_SCHEMA, planner);
        final Interpreter interpreter = new Interpreter(dataContext, bestExp);
        EnumeratorRowIterator enumeratorRowIterator = new EnumeratorRowIterator(CalciteConvertors.getMycatRowMetaData(bestExp.getRowType()), interpreter.enumerator());
        return new TextResultSetResponse(enumeratorRowIterator);
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


    public void flash() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(false);
        SchemaPlus dataNodes = rootSchema.add(MetadataManager.DATA_NODES, new AbstractSchema());
        for (Map.Entry<String, ConcurrentHashMap<String, MetadataManager.LogicTable>> stringConcurrentHashMapEntry : MetadataManager.INSTANCE.logicTableMap.entrySet()) {
            SchemaPlus schemaPlus = rootSchema.add(stringConcurrentHashMapEntry.getKey(), new AbstractSchema());
            for (Map.Entry<String, MetadataManager.LogicTable> entry : stringConcurrentHashMapEntry.getValue().entrySet()) {
                MetadataManager.LogicTable logicTable = entry.getValue();
                MycatLogicTable mycatLogicTable = new MycatLogicTable(logicTable);
                schemaPlus.add(entry.getKey(), mycatLogicTable);

                for (BackendTableInfo backend : logicTable.getBackends()) {
                    String uniqueName = backend.getUniqueName();
                    MycatPhysicalTable mycatPhysicalTable = new MycatPhysicalTable(mycatLogicTable, backend);
                    dataNodes.add(uniqueName, mycatPhysicalTable);
                }
            }
        }
        this.ROOT_SCHEMA = rootSchema;
    }

    public SchemaPlus getRootSchema() {
        return ROOT_SCHEMA;
    }
}
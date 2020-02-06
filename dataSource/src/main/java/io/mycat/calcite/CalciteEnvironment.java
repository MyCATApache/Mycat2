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
import io.mycat.calcite.relBuilder.MyRelBuilder;
import io.mycat.calcite.relBuilder.MycatTransientSQLTable;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public enum CalciteEnvironment {
    INSTANCE;
    final Logger LOGGER = LoggerFactory.getLogger(CalciteEnvironment.class);
    final static SqlParser.Config SQL_PARSER_CONFIG = SqlParser.configBuilder().setLex(Lex.MYSQL)
            .setConformance(SqlConformanceEnum.MYSQL_5)
            .setCaseSensitive(false).build();
    volatile SchemaPlus ROOT_SCHEMA;
    private final EnumMap<SqlKind, Boolean>
            SUPPORTED_AGGREGATES = new EnumMap<>(SqlKind.class);

    private CalciteEnvironment() {
        Driver driver = new Driver();//触发驱动注册
        final String charset = "UTF-8";
        System.setProperty("saffron.default.charset", charset);
        System.setProperty("saffron.default.nationalcharset", charset);
        System.setProperty("calcite.default.charset", charset);
        System.setProperty("saffron.default.collat​​ion.tableName", charset + "$ en_US");

        SUPPORTED_AGGREGATES.put(SqlKind.MIN, true);
        SUPPORTED_AGGREGATES.put(SqlKind.MAX, true);
        SUPPORTED_AGGREGATES.put(SqlKind.COUNT, true);
        SUPPORTED_AGGREGATES.put(SqlKind.SUM, true);
        SUPPORTED_AGGREGATES.put(SqlKind.SUM0, true);
        SUPPORTED_AGGREGATES.put(SqlKind.ANY_VALUE, true);
        SUPPORTED_AGGREGATES.put(SqlKind.BIT_AND, true);
        SUPPORTED_AGGREGATES.put(SqlKind.BIT_OR, true);
        flash();
    }

    public TextResultSetResponse getConnection(String defaultSchema, String sql) throws Exception {
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        if (defaultSchema != null) {
            configBuilder.defaultSchema(ROOT_SCHEMA.getSubSchema(defaultSchema));
        }
        configBuilder.parserConfig(SQL_PARSER_CONFIG);
        FrameworkConfig config = configBuilder.build();
        RelBuilder relBuilder = RelBuilder.create(config);
        PlannerImpl planner = new PlannerImpl(config);
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        RelNode convert = planner.convert(validate);

        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

        Arrays.asList(
                FilterTableScanRule.INSTANCE,
                ProjectTableScanRule.INSTANCE,
                FilterSetOpTransposeRule.INSTANCE,
 ProjectRemoveRule.INSTANCE,
//        planner2.addRule(JoinUnionTransposeRule.LEFT_UNION,
//        planner2.addRule(JoinUnionTransposeRule.RIGHT_UNION.
    JoinExtractFilterRule.INSTANCE,
//        planner2.addRule(JoinPushTransitivePredicatesRule.INSTANCE,
//                AggregateUnionTransposeRule.INSTANCE,
//                AggregateUnionAggregateRule.AGG_ON_FIRST_INPUT,
//                AggregateUnionAggregateRule.AGG_ON_SECOND_INPUT,
                AggregateUnionAggregateRule.INSTANCE,
                AggregateProjectMergeRule.INSTANCE,
                AggregateProjectPullUpConstantsRule.INSTANCE,
                PushDownLogicTable.INSTANCE_FOR_PushDownFilterLogicTable,
                AggregateValuesRule.INSTANCE
        ).forEach(i -> hepProgramBuilder.addRuleInstance(i));
//        hepProgramBuilder.addRuleInstance(PushDownLogicTable.INSTANCE_FOR_PushDownLogicTable);

        final HepPlanner planner2 = new HepPlanner(hepProgramBuilder.build());

        planner2.setRoot(convert);
        RelNode bestExp = planner2.findBestExp();
             RelShuttleImpl relShuttle = new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatLogicTable unwrap = scan.getTable().unwrap(MycatLogicTable.class);
                if (unwrap != null) {
                    return PushDownLogicTable.toPhyTable(relBuilder, scan);
                }
                return super.visit(scan);
            }
        };
        planner2.setRoot(bestExp);
        bestExp = planner2.findBestExp();

        bestExp = bestExp.accept(relShuttle);
        bestExp = relShuttle.visit(bestExp);

        //子节点运算的节点是同一个目标的,就把它们的父节点标记为可以变成SQL
        IdentityHashMap<RelNode, Boolean> cache = new IdentityHashMap<>();
        IdentityHashMap<RelNode, List<String>> margeList = new IdentityHashMap<>();
        RelHomogeneousShuttle relHomogeneousShuttle = new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode res = super.visit(other);//后续遍历
                List<RelNode> inputs = other.getInputs();
                boolean isLeftNode = inputs == null || other.getInputs() != null && other.getInputs().isEmpty();

                if (!isLeftNode) {
                    ArrayList<String> targetList = new ArrayList<>();
                    for (RelNode input : inputs) {
                        targetList.addAll(margeList.getOrDefault(input, Collections.emptyList()));
                    }
                    Set<String> distinct = new HashSet<>(targetList);
                    margeList.put(other, targetList);
                    cache.put(other, distinct.isEmpty() || distinct.size() == 1);
                } else {
                    MycatPhysicalTable mycatPhysicalTable = other.getTable().unwrap(MycatPhysicalTable.class);
                    if (mycatPhysicalTable != null) {
                        margeList.put(other, Collections.singletonList(mycatPhysicalTable.getTargetName()));
                    } else {
                        margeList.put(other, Collections.emptyList());
                    }
                    cache.put(other, Boolean.TRUE);
                }
                return res;
            }
        };
        bestExp.accept(relHomogeneousShuttle);
        relHomogeneousShuttle.visit(bestExp);


        bestExp = new RelShuttleImpl() {
            @Override
            protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                if (parent instanceof Aggregate && child instanceof Union) {
                    Aggregate aggregate = (Aggregate) parent;
                    List<AggregateCall> aggCallList = aggregate.getAggCallList();
                    boolean allMatch = aggCallList.stream().allMatch(aggregateCall -> SUPPORTED_AGGREGATES.getOrDefault(aggregateCall.getAggregation().getKind(), false));
                    if (allMatch) {
                        List<RelNode> inputs = child.getInputs();
                        List<RelNode> resList = new ArrayList<>(inputs.size());
                        for (RelNode input : inputs) {
                            RelNode res;
                            if (cache.get(input)) {
                                res = LogicalAggregate.create(input, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
                                planner2.setRoot(res);
                                res = planner2.findBestExp();
                                cache.put(res, Boolean.TRUE);
                                margeList.put(res, margeList.get(input));
                            } else {
                                res = input;
                            }
                            resList.add(res);
                        }
                        LogicalUnion logicalUnion = LogicalUnion.create(resList, ((Union) child).all);
                        return LogicalAggregate.create(logicalUnion, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
                    }
                }
                return super.visitChild(parent, i, child);
            }
        }.visit(bestExp);


        //从根节点开始把变成SQL下推
        RelHomogeneousShuttle relHomogeneousShuttle1 = new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                if (cache.get(other) == Boolean.TRUE) {
                    List<String> strings = margeList.get(other);
                    String targetName = strings.get(0);
                    return MyRelBuilder.makeTransientSQLScan(relBuilder, targetName, other);
                }
                return super.visit(other);
            }
        };
        bestExp = bestExp.accept(relHomogeneousShuttle1);
        //收集MycatTransientSQLTable
        List<MycatTransientSQLTable> list = new ArrayList<>();
        bestExp.accept(new RelShuttleImpl() {

            @Override
            public RelNode visit(TableScan scan) {
                MycatTransientSQLTable unwrap = scan.getTable().unwrap(MycatTransientSQLTable.class);
                if (unwrap != null) {
                    list.add(unwrap);
                } else {
                    throw new IllegalArgumentException();
                }
                return super.visit(scan);
            }
        });
//        RelShuttleImpl projectShuttle = new RelHomogeneousShuttle() {
//
//            @Override
//            public RelNode visit(RelNode other) {
//                RelDataType rowType = other.getRowType();
//                RelOptUtil.c()
//                return super.visit(other);
//            }
//        };
//       bestExp =  bestExp.accept(projectShuttle);
//        bestExp=    projectShuttle.visit(bestExp);
        try {
            List<String> explain = MetadataManager.INSTANCE.explain(bestExp);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
//package io.mycat.lib.impl;
//
//import io.mycat.MycatException;
//import io.mycat.beans.resultset.MycatResultSetResponse;
//import io.mycat.calcite.CalciteEnvironment;
//import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
//import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
//import org.apache.calcite.config.Lex;
//import org.apache.calcite.jdbc.CalciteConnection;
//import org.apache.calcite.plan.hep.HepPlanner;
//import org.apache.calcite.plan.hep.HepProgram;
//import org.apache.calcite.plan.hep.HepProgramBuilder;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.core.JoinRelType;
//import org.apache.calcite.rel.externalize.RelJsonWriter;
//import org.apache.calcite.rel.externalize.RelWriterImpl;
//import org.apache.calcite.rel.externalize.RelXmlWriter;
//import org.apache.calcite.rel.rules.CalcSplitRule;
//import org.apache.calcite.rel.rules.FilterTableScanRule;
//import org.apache.calcite.rel.rules.ProjectTableScanRule;
//import org.apache.calcite.schema.SchemaPlus;
//import org.apache.calcite.sql.SqlExplainLevel;
//import org.apache.calcite.sql.parser.SqlParser;
//import org.apache.calcite.tools.FrameworkConfig;
//import org.apache.calcite.tools.Frameworks;
//import org.apache.calcite.tools.RelBuilder;
//import org.apache.calcite.tools.RelRunner;
//
//import java.io.PrintWriter;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.function.Supplier;
//
//public enum CalciteLib {
//    INSTANCE;
//
//    public Response responseQueryCalcite(String sql) {
//        return JdbcLib.response(queryCalcite(sql));
//    }
//
//    public Supplier<MycatResultSetResponse[]> queryCalcite(String sql) {
//        return () -> {
//            try {
//                CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection();
//                Statement statement = null;
//                statement = connection.createStatement();
//                ResultSet resultSet = statement.executeQuery(sql);
//                JdbcRowBaseIteratorImpl jdbcRowBaseIterator = new JdbcRowBaseIteratorImpl(statement, resultSet);
//                return new MycatResultSetResponse[]{new TextResultSetResponse(jdbcRowBaseIterator)};
//            } catch (Exception e) {
//                throw new MycatException(e);
//            }
//        };
//    }
//
//    public Supplier<MycatResultSetResponse[]> queryCalcite(RelNode rootRel) {
//        return () -> {
//                CalciteConnection connection = CalciteEnvironment.INSTANCE.getConnection();
//                try {
//                    PreparedStatement  statement = connection.unwrap(RelRunner.class).prepare(rootRel);
//                    ResultSet resultSet = statement.executeQuery();
//                    JdbcRowBaseIteratorImpl jdbcRowBaseIterator = new JdbcRowBaseIteratorImpl(statement, resultSet);
//                    return new MycatResultSetResponse[]{new TextResultSetResponse(jdbcRowBaseIterator)};
//                } catch (SQLException e) {
//                    throw new MycatException(e);
//                }
//        };
//    }
//
//    public Response responseTest() {
//        return JdbcLib.response(test());
//    }
//
//    public Supplier<MycatResultSetResponse[]> test() {
//        CalciteConnection connection = CalciteEnvironment.INSTANCE.getRawConnection();
//        SchemaPlus rootSchema1 = connection.getRootSchema();
//        CalciteEnvironment.INSTANCE.setSchemaMap(rootSchema1);
//        final FrameworkConfig config = Frameworks.newConfigBuilder()
//                .parserConfig(SqlParser.configBuilder().setCaseSensitive(false).setLex(Lex.MYSQL).build())
//                .defaultSchema(rootSchema1)
//                .build();
////        try {
////            SqlNode sqlNode = SqlParser.create("1+1").parseExpression();
////            Planner planner = Frameworks.getPlanner(config);
////            RelRoot rel = planner.rel(sqlNode);
////            System.out.println(sqlNode);
////        } catch (SqlParseException e) {
////            e.printStackTrace();
////        } catch (RelConversionException e) {
////            e.printStackTrace();
////        }
//
//        /**
//         *
//         *
//         * t.id =  testdb.travelrecord.id
//         * t.user_id =  testdb.travelrecord.user_id
//         * a.id = testdb.address.id
//         *
//         * LogicalProject(id=[t.id], user_id=[t.user_id])
//         *   LogicalJoin(condition=[=(t.id , t.user_id)], joinType=[inner])
//         *     LogicalProject(id=[t.id], user_id=[t.user_id])
//         *       LogicalTableScan(table=[[testdb, travelrecord]])
//         *     LogicalFilter(condition=[=($0, 1)])
//         *       LogicalProject(id=[a.id])
//         *         LogicalTableScan(table=[[testdb, address]])
//         */
//        RelBuilder relBuilder = RelBuilder.create(config);
//        RelNode table = relBuilder
//                .scan("testdb","travelrecord")
//                .as("t")
//                .scan("testdb","address")
//                .as("a")
//                .join(JoinRelType.INNER, relBuilder.equals(relBuilder.field(2,0,0),
//                        relBuilder.field(2,1,1)))
//                .filter(relBuilder.and(relBuilder.equals(relBuilder.field(1,0,0),relBuilder.literal(1))))
//                .project(relBuilder.field(1,0,0), relBuilder.field(1,0,1))
//                .build();
//        RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES,false);
//         table.explain(relWriter);
//        System.out.println(relWriter.toString());
//        table.explain(new RelXmlWriter(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES));
//        RelJsonWriter jsonWriter;
//        table.explain(jsonWriter =new RelJsonWriter());
//        System.out.println(jsonWriter);
//        final HepProgram hepProgram = new HepProgramBuilder()
//                .addRuleInstance(CalcSplitRule.INSTANCE)
//                .addRuleInstance(FilterTableScanRule.INSTANCE)
//                .addRuleInstance(FilterTableScanRule.INTERPRETER)
//                .addRuleInstance(ProjectTableScanRule.INSTANCE)
//                .addRuleInstance(ProjectTableScanRule.INTERPRETER).build();
//        final HepPlanner planner = new HepPlanner(hepProgram);
//        planner.setRoot(table);
//        table = planner.findBestExp();
//        System.out.println(table);
//        Supplier<MycatResultSetResponse[]> supplier = queryCalcite(table);
//        return supplier;
//    }
//}
package io.mycat.hbt;


import io.mycat.calcite.prepare.MycatHbtCalcitePrepareObject;
import io.mycat.hbt.ast.HBTBuiltinHelper;
import io.mycat.hbt.ast.HBTOp;
import io.mycat.hbt.ast.HBTTypes;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.query.JoinSchema;
import io.mycat.hbt.ast.query.RenameSchema;
import io.mycat.hbt.parser.HBTParser;
import io.mycat.upondb.MycatDBClientBasedConfig;
import lombok.SneakyThrows;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.mycat.hbt.ast.HBTOp.CORRELATE_INNER_JOIN;
import static io.mycat.hbt.ast.HBTOp.CORRELATE_LEFT_JOIN;


public class HBTBaseTest implements HBTBuiltinHelper {
    private final MockDBClientMediator clientMediator = new MockDBClientMediator(MycatDBClientBasedConfig.
            builder().reflectiveSchemas(Collections.singletonMap("db1", new Db1())).build());

    public RelNode toRelNode(Schema node) {
        MycatHbtCalcitePrepareObject mycatHbtCalcitePrepareObject = new MycatHbtCalcitePrepareObject(null, 0, node, clientMediator);
        return mycatHbtCalcitePrepareObject.getRelNode(Collections.emptyList());
    }

    @Test
    public void selectWithoutFrom() throws IOException {
        String sugar = "table(fields(fieldType(`1`,`integer`,false)),values())";
        String desugar = sugar;
        Schema code = table(Arrays.asList(fieldType("1", HBTTypes.Integer,false)), Arrays.asList());
        testText(sugar, desugar, code, "LogicalValues(tuples=[[]])");
        testDumpResultSet(code, "");
    }


    private void testSchema(Schema schema, String expect) {
        RelNode relNode = toRelNode(schema);
        testRel(relNode, expect);
    }

    private void testRel(RelNode relNode, String expect) {
        Assert.assertEquals(
                expect.replace("\n", "").replace("\r", "").trim()
                ,
                RelOptUtil.toString(relNode, SqlExplainLevel.EXPPLAN_ATTRIBUTES)
                        .replace("\n", "").replace("\r", "").trim()
        );
    }

    //
    private Schema toDSL(RelNode relNode) {
        return RelNodeConvertor.convertRelNode(relNode);
    }

    private void testText(String sugarText, String desugarText, Schema schema, String relText) {
        String normalSugarText = toNormalString(sugarText);
        String normalDesugarText = toNormalString(desugarText);
        Assert.assertEquals(normalSugarText, normalDesugarText);

        String codeText = toNormalString(schema);
        Assert.assertEquals(normalSugarText, codeText);
        Assert.assertEquals(normalDesugarText, toNormalString(transfor(normalDesugarText)));

        testRel(schema, relText, normalSugarText);
    }

    private void testRel(Schema schema, String relText, String normalSugarText) {
        RelNode relNode = toRelNode(schema);
        Schema schema1 = toDSL(relNode);
        Assert.assertEquals(normalSugarText, toNormalString(schema1));

        Assert.assertEquals(
                relText.replace("\n", "").replace("\r", "").trim()
                ,
                RelOptUtil.toString(relNode, SqlExplainLevel.EXPPLAN_ATTRIBUTES)
                        .replace("\n", "").replace("\r", "").trim()
        );
    }

    @Test
    public void selectWithoutFrom2() throws IOException {
        String sugar = "table(fields(fieldType(id,integer,false)),values(1))";
        String desugar = sugar;
        Schema code = table(Arrays.asList(fieldType("id", HBTTypes.Integer,false)), Arrays.asList(1));
        testText(sugar, desugar, code, "LogicalValues(tuples=[[{ 1 }]])");
        testDumpResultSet(code, "(1)");

    }


    @Test
    public void selectDistinctWithoutFrom() throws IOException {
        String sugar = "fromTable(db1,travelrecord) unionDistinct  fromTable(`db1`, `travelrecord`)";
        String desugar = "unionDistinct(fromTable(`db1`,`travelrecord`),fromTable(`db1`,`travelrecord`))";
        Schema code = set(HBTOp.UNION_DISTINCT, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")));

        testText(sugar, desugar, code, "LogicalUnion(all=[false])  LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");
        testDumpResultSet(code, "(1,10)(2,20)");
    }


    @Test
    public void selectDistinctWithoutFrom2() throws IOException {
        String sugar = "fromTable(db1,travelrecord).distinct()";
        String desugar = "distinct(fromTable(db1,travelrecord))";
        Schema code = distinct(fromTable("db1", "travelrecord"));
        testText(sugar, desugar, code);
        testSchema(code, "LogicalAggregate(group=[{0, 1}])  LogicalTableScan(table=[[db1, travelrecord]])");
        testDumpResultSet(code, "(2,20)\n" +
                "(1,10)");

        testDumpResultSet(transfor("distinct(table(fields(fieldType(id,integer)),values(1,1)))"), "(1)");
    }

    @Test
    public void selectProjectItemWithoutFrom() throws IOException {
        String sugar = "table(fields(fieldType(`1`,`integer`,true),fieldType(`2`,`integer`,true)),values(1,2)).rename(`2`,`1`)";
        String desugar = "rename(table(fields(fieldType(`1`,`integer`,true),fieldType(`2`,`integer`,true)),values(1,2)),`2`,`1`)";
        Schema code = rename(table(Arrays.asList(fieldType("1", "integer",true), fieldType("2", "integer",true)), Arrays.asList(1,2)), Arrays.asList("2", "1"));
        testText(sugar, desugar, code);

        testDumpResultSet(transfor(sugar), "(1,2)");
    }

    private Schema rename(Schema table, List<String> asList) {
        return new RenameSchema(table, asList);
    }

    @Test
    public void selectProjectItemWithoutFrom2() throws IOException {
        String sugar = "table(fields(fieldType(id,integer),fieldType(id2,integer)),values()).map(id2 as id4)";
        String desugar = "map(table(fields(fieldType(id,integer),fieldType(id2,integer)),values()),id2 as id4)";

        Schema code = map(table(Arrays.asList(fieldType("id", "integer",true), fieldType("id2", "integer",true)), Arrays.asList()), Arrays.asList(as(new Identifier("id2"), new Identifier("id4"))));
        testText(sugar, desugar, code, "LogicalProject(id4=[$1])  LogicalValues(tuples=[[]])");

        testDumpResultSet(transfor(sugar), "");
    }

    @Test
    public void testFrom() throws IOException {
        String text = "fromTable(db1,travelrecord)";
        testText(text, text, fromTable("db1", "travelrecord"), "LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(transfor(text), "(1,10)\n" +
                "(2,20)");
    }

    @Test
    public void selectProjectFrom() throws IOException {
        String sugar = "fromTable(db1,travelrecord).map(id as id0)";
        String desugar = "map(fromTable(db1,travelrecord),id as id0)";
        Schema code = map(fromTable("db1", "travelrecord"), Arrays.asList(as(new Identifier("id"), new Identifier("id0"))));
        testText(sugar, desugar, code, "LogicalProject(id0=[$0])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(transfor(sugar), "(1)\n" +
                "(2)");
    }

    private Expr as(Expr id, Expr id0) {
        return new Expr(HBTOp.AS_COLUMN_NAME, id, id0);
    }


    @Test
    public void selectUnionAll() throws IOException {
        String sugar = "fromTable(db1,travelrecord) unionAll  fromTable(db1,travelrecord)";
        Schema code = set(HBTOp.UNION_ALL, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")));
        String text = "unionAll(fromTable(db1,travelrecord),fromTable(db1,travelrecord))";

        testText(sugar, text, code, "LogicalUnion(all=[true])  LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(transfor(sugar), "(1,10)\n" +
                "(2,20)\n" +
                "(1,10)\n" +
                "(2,20)");
    }

    @Test
    public void selectUnionDistinct() throws IOException {
        String sugar = "fromTable(db1,travelrecord) unionDistinct fromTable(db1,travelrecord)";
        Schema code = set(HBTOp.UNION_DISTINCT, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")));
        String text = "unionDistinct(fromTable(db1,travelrecord),fromTable(db1,travelrecord))";

        testText(sugar, text, code, "LogicalUnion(all=[false])  LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(transfor(sugar), "(1,10)(2,20)");
    }


    @Test
    public void selectExceptDistinct() throws IOException {
        String sugar = "fromTable(db1,travelrecord) exceptDistinct fromTable(db1,travelrecord)";
        Schema code = set(HBTOp.EXCEPT_DISTINCT, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")));
        String text = "exceptDistinct(fromTable(db1,travelrecord),fromTable(db1,travelrecord))";

        testText(sugar, text, code, "LogicalMinus(all=[false])  LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(transfor(sugar), "");
    }

    @Test
    public void selectExceptAll() throws IOException {
        String sugar = "fromTable(db1,travelrecord) exceptAll fromTable(db1,travelrecord)";
        Schema code = set(HBTOp.EXCEPT_ALL, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")));
        String text = "exceptAll(fromTable(db1,travelrecord),fromTable(db1,travelrecord))";

        testText(sugar, text, code, "LogicalMinus(all=[true])  LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(transfor("exceptAll( fromTable(db1,travelrecord), table(fields(fieldType(id,integer),fieldType(id2,integer)),values()))"), "(1,10)\n" +
                "(2,20)");
    }

    @Test
    public void selectFromOrder() throws IOException {
        Schema schema = orderBy(fromTable("db1", "travelrecord"), Arrays.asList(order("id", Direction.ASC), order("user_id", Direction.DESC)));
        String sugar = "fromTable(db1,travelrecord).orderBy(order(id,ASC), order(user_id,DESC))";
        String text = "orderBy(fromTable(db1,travelrecord),order(id,ASC), order(user_id,DESC))";
        testText(sugar, text, schema, "LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])  LogicalTableScan(table=[[db1, travelrecord]])");
    }

    @Test
    public void selectFromLimit() throws IOException {
        Schema schema = limit(fromTable("db1", "travelrecord"), 1, 2);
        String sugar = "fromTable(db1,travelrecord).limit(1,2)";
        String text = "limit(fromTable(db1,travelrecord),1,2)";
        testText(sugar, text, schema, "LogicalSort(offset=[1], fetch=[2])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(schema, "(2,20)");
    }

    @Test
    public void selectFromGroupByKey() throws IOException {
        String sugar = "fromTable(db1,travelrecord).groupBy(keys(groupKey(`id`))))";
        String text = "groupBy(fromTable(db1,travelrecord),keys(groupKey(`id`)))";
        Schema schema = groupBy(fromTable("db1", "travelrecord"), Arrays.asList(groupKey(Arrays.asList(id(("id"))))), Arrays.asList());
        testText(sugar, text, schema, "LogicalAggregate(group=[{0}])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(schema, "(1)\n" +
                "(2)");
    }

    @Test
    public void selectFromGroupByKeyAvg() throws IOException {
        String sugar = "fromTable(db1,travelrecord).groupBy(keys(groupKey(`id`)),aggregating(avg(`id`).alias(a).distinct().approximate().ignoreNulls().filter(true).orderBy(order(user_id,DESC))))";
        String text = "groupBy(fromTable(db1,travelrecord),keys(groupKey(`id`)),aggregating(avg(`id`).alias(a).distinct().approximate().ignoreNulls().filter(true).orderBy(order(user_id,DESC))))";
        Schema schema = groupBy(fromTable("db1", "travelrecord"), Arrays.asList(groupKey(Arrays.asList(id(("id"))))),
                Arrays.asList(new AggregateCall("avg", Arrays.asList(id(("id")))).alias("a").distinct().approximate().ignoreNulls().filter(literal(true))
                        .orderBy(Arrays.asList(order("user_id", Direction.DESC)))));
        testText(sugar, text, schema);

        testSchema(schema, "LogicalAggregate(group=[{0}], agg#0=[AVG(DISTINCT $0) WITHIN GROUP ([1 DESC]) FILTER $2])  LogicalProject(id=[$0], user_id=[$1], $f2=[true])    LogicalTableScan(table=[[db1, travelrecord]])");


        testDumpResultSet(schema, "(1,1.0)(2,2.0)");
    }


    private Expr literal(Object b) {
        return new Literal(b);
    }

    @Test
    public void selectFromGroupByKeyAvg2() throws IOException {
        String sugar = "fromTable(db1,travelrecord).groupBy(keys(groupKey(`id`),groupKey(`user_id`)),aggregating(avg(`id`),avg(`user_id`)))";
        String text = "groupBy(fromTable(db1,travelrecord),keys(groupKey(`id`),groupKey(`user_id`)),aggregating(avg(`id`),avg(`user_id`)))";
        Schema schema = groupBy(fromTable("db1", "travelrecord"), Arrays.asList(
                groupKey(Arrays.asList(id(("id")))),
                groupKey(Arrays.asList(id(("user_id"))))),
                Arrays.asList(
                        new AggregateCall("avg", Arrays.asList(id(("id")))),
                        new AggregateCall("avg", Arrays.asList(id(("user_id"))))
                ));
        testText(sugar, text, schema);
        testSchema(schema, "LogicalAggregate(group=[{0, 1}], groups=[[{0}, {1}]], agg#0=[AVG($0)], agg#1=[AVG($1)])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(schema, "(1,null,1.0,10.0)(null,20,2.0,20.0)(2,null,2.0,20.0)(null,10,1.0,10.0)");
    }

    private Identifier id(String id) {
        return new Identifier(id);
    }

    @Test
    public void selectCastFrom() throws IOException {
        String sugar = "fromTable(db1,travelrecord).map(cast(1,float) as a)";
        String text = "map(fromTable(db1,travelrecord),cast(1,float) as a)";
        Schema schema = map(fromTable("db1", "travelrecord"), Arrays.asList(as(cast(new Literal(1), new Identifier("float")), new Identifier("a"))));
        testText(sugar, text, schema);

        testDumpResultSet(schema, "(1.0)\n" +
                "(1.0)");
    }

    private Expr cast(Literal literal, Identifier aFloat) {
        return new Expr(HBTOp.CAST, Arrays.asList(literal, aFloat));
    }

    @Test
    public void selectUcaseFrom() throws IOException {
        String sugar = "table(fields(fieldType(id,varchar,false)),values('A')).map(lower(`id`))";
        String text = "map(table(fields(fieldType(id,varchar,false)),values('A')),lower(`id`))";
        Schema map = map(table(Arrays.asList(fieldType("id", "varchar",false)), Arrays.asList("A")), Arrays.asList(new Fun("lower", Arrays.asList(new Identifier("id")))));
        testText(sugar, text, map);
        testSchema(map, "LogicalProject($f0=[LOWER($0)])  LogicalValues(tuples=[[{ 'A' }]])");

        testDumpResultSet(map, "(a)");
    }

    @Test
    public void filterIn() throws IOException {
        String sugar = "fromTable(db1,travelrecord).filter(`id` = 1)";
        String text = "filter(fromTable(db1,travelrecord),`id` = 1)";
        Schema schema = filter(fromTable("db1", "travelrecord"), eq(new Identifier("id"), new Literal(1)));
        testText(sugar, text, schema);
        testSchema(schema, "LogicalFilter(condition=[=($0, 1)])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(schema, "(1,10)");
    }

    @Test
    public void filterAndOr() throws IOException {
        String sugar = "fromTable(db1,travelrecord).filter(`id` = 1 or `id` = 2 or `id` = 3)";
        String text = "filter(fromTable(db1,travelrecord),`id` = 1 or `id` = 2 or `id` = 3 )";
        Schema schema = filter(fromTable("db1", "travelrecord"),
                or(
                        or(
                                eq(new Identifier("id"), new Literal(1)),
                                eq(new Identifier("id"), new Literal(2)))
                        , eq(new Identifier("id"), new Literal(3))

                )
        );
        testText(sugar, text, schema);
        testSchema(schema, "LogicalFilter(condition=[OR(=($0, 1), =($0, 2), =($0, 3))])  LogicalTableScan(table=[[db1, travelrecord]])");
        testDumpResultSet(schema, "(1,10)\n" +
                "(2,20)");
    }

    @Test
    public void filterAndOr2() throws IOException {
        String sugar = "fromTable(db1,travelrecord).filter(`id` = 1 or `id` = 2 and `id` = 3)";
        String text = "filter(fromTable(db1,travelrecord),`id` = 1 or `id` = 2 and `id` = 3 )";
        Schema schema = filter(fromTable("db1", "travelrecord"),
                or(eq(new Identifier("id"), new Literal(1)),
                        and(
                                eq(new Identifier("id"), new Literal(2)),
                                eq(new Identifier("id"), new Literal(3)))

                )
        );
        testText(sugar, text, schema);
        testSchema(schema, "LogicalFilter(condition=[=($0, 1)])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(schema, "(1,10)");
    }

    @Test
    public void testInnerJoin() throws IOException {
        String sugar = "innerJoin(`id0` = `id`,fromTable(db1,travelrecord).map(`id` as `id0`),fromTable(db1,travelrecord))";
        Schema schema = new JoinSchema(HBTOp.INNER_JOIN,
                eq(id("id0"), id("id"))
                , map(fromTable("db1", "travelrecord"), Arrays.asList(as(new Identifier("id"), new Identifier("id0")))),
                fromTable("db1", "travelrecord")
        );
        testText(sugar, sugar, schema);
        testSchema(schema, "LogicalJoin(condition=[=($1, $0)], joinType=[inner])  LogicalProject(id0=[$0])    LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");
        testDumpResultSet(schema, "(1,1,10)\n" +
                "(2,2,20)");
    }

    @Test
    public void testCorrelateInnerJoCorrelateSchemain() throws IOException {
        String sugar = "correlateInnerJoin(`t`,table(fields(fieldType(id0,integer,false)),values(1,2,3,4)) , fromTable(`db1`,`travelrecord`).filter(ref(`t`,`id0`) = `id`)))";
        Schema db0 = table(Arrays.asList(fieldType("id0", HBTTypes.Integer,false)), Arrays.asList(1, 2, 3, 4));
        Schema db1 = filter(fromTable("db1", "travelrecord"), eq(ref("t", "id0"), new Identifier("id")));
        Schema schema = correlate(CORRELATE_INNER_JOIN, "t", db0, db1);
        testText(sugar, sugar, schema);
        testSchema(schema, "LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])  LogicalValues(tuples=[[{ 1 }, { 2 }, { 3 }, { 4 }]])  LogicalFilter(condition=[=($cor0.id0, $0)])    LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(schema, "(1,1,10)\n" +
                "(2,2,20)");
    }
    @Test
    public void testCorrelateLeftJoCorrelateSchemain() throws IOException {
        String sugar = "correlateLeftJoin(`t`,table(fields(fieldType(id0,integer,false)),values(1,2,3,4)) , fromTable(`db1`,`travelrecord`).filter(ref(`t`,`id0`) = `id`)))";
        Schema db0 = table(Arrays.asList(fieldType("id0", HBTTypes.Integer,false)), Arrays.asList(1, 2, 3, 4));
        Schema db1 = filter(fromTable("db1", "travelrecord"), eq(ref("t", "id0"), new Identifier("id")));
        Schema schema = correlate(CORRELATE_LEFT_JOIN, "t", db0, db1);
        testText(sugar, sugar, schema);
        testSchema(schema, "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])  LogicalValues(tuples=[[{ 1 }, { 2 }, { 3 }, { 4 }]])  LogicalFilter(condition=[=($cor0.id0, $0)])    LogicalTableScan(table=[[db1, travelrecord]])");

        testDumpResultSet(schema, "(1,1,10)(2,2,20)(3,null,null)(4,null,null)");
    }
    public static Expr ref(String corName, String fieldName) {
        return new Expr(HBTOp.REF, new Identifier(corName), new Identifier(fieldName));
    }

    private Expr eq(Expr id, Expr i) {
        return new Fun("eq", Arrays.asList(id, i));
    }

    private Expr or(Expr... ids) {
        return new Fun("or", Arrays.asList(ids));
    }

    private Expr and(Expr id, Expr i) {
        return new Fun("and", Arrays.asList(id, i));
    }


    private GroupKey groupKey(List<Expr> exprList) {
        return new GroupKey(exprList);
    }

    private void testText(String sugarText, String desugarText, Schema schema) {
        String normalSugarText = toNormalString(sugarText);
        String normalDesugarText = toNormalString(desugarText);
        Assert.assertEquals(normalSugarText, normalDesugarText);

        String codeText = toNormalString(schema);
        Assert.assertEquals(normalSugarText, codeText);
        Assert.assertEquals(normalDesugarText, toNormalString(transfor(normalDesugarText)));
    }

    private static String toNormalString(String text) {
        Schema transfor = transfor(text);
        return toNormalString(transfor);
    }

    private static String toNormalString(Schema desugar) {
        ExplainVisitor explainVisitor = new ExplainVisitor();
        desugar.accept(explainVisitor);
        return explainVisitor.getString();
    }

    private static Schema transfor(String text) {
        HBTParser hbtParser = new HBTParser(text);
        SchemaConvertor schemaConvertor = new SchemaConvertor();
        return schemaConvertor.transforSchema(hbtParser.statement());
    }
    @SneakyThrows
    public void testDumpResultSet(Schema schema, String resultset) {
        HBTRunners hbtRunners = new HBTRunners(clientMediator);
        Assert.assertEquals(resultset.replaceAll("\r","").replaceAll("\n","").trim(), TextConvertor.dumpResultSet(hbtRunners.run(schema)).replaceAll("\r","").replaceAll("\n","").trim());
    }

    @SneakyThrows
    public void testDumpResultSetColumn(Schema schema, String resultset) {
        HBTRunners hbtRunners = new HBTRunners(clientMediator);
        Assert.assertEquals(resultset.replaceAll("\r","").replaceAll("\n","").trim(), TextConvertor.dumpResultSet(hbtRunners.run(schema)).replaceAll("\r","").replaceAll("\n","").trim());
    }
}
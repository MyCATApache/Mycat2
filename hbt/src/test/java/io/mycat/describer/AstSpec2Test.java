package io.mycat.describer;


import io.mycat.calcite.MycatRelBuilder;
import io.mycat.hbt.*;
import io.mycat.hbt.ast.AggregateCall;
import io.mycat.hbt.ast.Direction;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.query.JoinSchema;
import io.mycat.hbt.ast.query.RenameSchema;
import io.mycat.hbt.parser.HBTParser;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static io.mycat.hbt.Op.CORRELATE_INNER_JOIN;
import static io.mycat.hbt.SchemaConvertor.*;


public class AstSpec2Test {
    private static final FrameworkConfig config = Frameworks.newConfigBuilder()
            .defaultSchema(Frameworks.createRootSchema(true).add("db1", new ReflectiveSchema(new Db1()))).build();

    public RelNode toRelNode(Schema node) {
        return new HBTConvertor(MycatRelBuilder.create(config)).complie(node);
    }

    @Test
    public void selectWithoutFrom() throws IOException {
        String sugar = "table(fields(fieldType(`1`,`int`)),values())";
        String desugar = sugar;
        Schema code = table(Arrays.asList(fieldType("1", "int")), Arrays.asList());
        testText(sugar, desugar, code,"LogicalValues(tuples=[[]])");
        testDump(code,"");
    }

    private void testSchema(Schema schema, String s) {
        RelNode relNode = toRelNode(schema);
        testRel(relNode, s);
    }

    private void testRel(RelNode relNode, String s) {
        Assert.assertEquals(
                s.replace("\n", "").replace("\r", "").trim()
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
        String sugar = "table(fields(fieldType(id,int)),values(1))";
        String desugar = sugar;
        Schema code = table(Arrays.asList(fieldType("id", "int")), Arrays.asList(1));
        testText(sugar, desugar, code,"LogicalValues(tuples=[[{ 1 }]])");
        testDump(code,"(1)");

    }
    public  void testDump(Schema schema,String resultset) {
        String dump = TextConvertor.dump(toRelNode(schema));
        Assert.assertEquals(resultset.trim(),dump.trim());
    }


    @Test
    public void selectDistinctWithoutFrom() throws IOException {
        String sugar = "fromTable(db1,travelrecord) unionDistinct  fromTable(`db1`, `travelrecord`)";
        String desugar = "unionDistinct(fromTable(`db1`,`travelrecord`),fromTable(`db1`,`travelrecord`))";
        Schema code = set(Op.UNION_DISTINCT, Arrays.asList(SchemaConvertor.fromTable("db1", "travelrecord"), SchemaConvertor.fromTable("db1", "travelrecord")));

        testText(sugar, desugar, code,"LogicalUnion(all=[false])  LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");
        testDump(code,"(2,20)\n" +
                "(1,10)");
    }


    @Test
    public void selectDistinctWithoutFrom2() throws IOException {
        String sugar = "fromTable(db1,travelrecord).distinct()";
        String desugar = "distinct(fromTable(db1,travelrecord))";
        Schema code = distinct(fromTable("db1","travelrecord"));
        testText(sugar, desugar, code);
        testSchema(code,"LogicalAggregate(group=[{0, 1}])  LogicalTableScan(table=[[db1, travelrecord]])");
        testDump(code,"(2,20)\n" +
                "(1,10)");

        testDump(transfor("distinct(table(fields(fieldType(id,int)),values(1,1)))"),"(1)");
    }

    @Test
    public void selectProjectItemWithoutFrom() throws IOException {
        String sugar = "table(fields(fieldType(`1`,`int`),fieldType(`2`,`varchar`)),values()).rename(`2`,`1`)";
        String desugar = "rename(table(fields(fieldType(`1`,`int`),fieldType(`2`,`varchar`)),values()),`2`,`1`)";
        Schema code = rename(table(Arrays.asList(fieldType("1", "int"), fieldType("2", "varchar")), Arrays.asList()), Arrays.asList("2", "1"));
        testText(sugar, desugar, code);
        testSchema(code,"LogicalProject(2=[$0], 1=[$1])  LogicalValues(tuples=[[]])");

        testDump(transfor(sugar),"");
    }

    private Schema rename(Schema table, List<String> asList) {
        return new RenameSchema(table,asList);
    }

    @Test
    public void selectProjectItemWithoutFrom2() throws IOException {
        String sugar = "table(fields(fieldType(id,int),fieldType(id2,int)),values()).map(id2 as id4)";
        String desugar = "map(table(fields(fieldType(id,int),fieldType(id2,int)),values()),id2 as id4)";

        Schema code = map(table(Arrays.asList(fieldType("id", "int"), fieldType("id2", "int")), Arrays.asList()),Arrays.asList(as(new Identifier("id2"), new Identifier("id4"))));
        testText(sugar, desugar, code,"LogicalProject(id4=[$1])  LogicalValues(tuples=[[]])");

        testDump(transfor(sugar),"");
    }

    @Test
    public void testFrom() throws IOException {
        String text = "fromTable(db1,travelrecord)";
        testText(text, text, fromTable("db1", "travelrecord"),"LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(transfor(text),"(1,10)\n" +
                "(2,20)");
    }

    @Test
    public void selectProjectFrom() throws IOException {
        String sugar = "fromTable(db1,travelrecord).map(id as id0)";
        String desugar = "map(fromTable(db1,travelrecord),id as id0)";
        Schema code = map(fromTable("db1", "travelrecord"), Arrays.asList(as(new Identifier("id"),new Identifier("id0"))));
        testText(sugar, desugar, code,"LogicalProject(id0=[$0])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(transfor(sugar),"(1)\n" +
                "(2)");
    }

    private Expr as(Expr id, Expr id0) {
        return new Expr(Op.AS_COLUMNNAME,id,id0);
    }


    @Test
    public void selectUnionAll() throws IOException {
        String sugar = "fromTable(db1,travelrecord) unionAll  fromTable(db1,travelrecord)";
        Schema code = set(Op.UNION_ALL, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")));
        String text = "unionAll(fromTable(db1,travelrecord),fromTable(db1,travelrecord))";

        testText(sugar, text, code,"LogicalUnion(all=[true])  LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(transfor(sugar),"(1,10)\n" +
                "(2,20)\n" +
                "(1,10)\n" +
                "(2,20)");
    }

    @Test
    public void selectUnionDistinct() throws IOException {
        String sugar = "fromTable(db1,travelrecord) unionDistinct fromTable(db1,travelrecord)";
        Schema code = set(Op.UNION_DISTINCT, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")));
        String text = "unionDistinct(fromTable(db1,travelrecord),fromTable(db1,travelrecord))";

        testText(sugar, text, code,"LogicalUnion(all=[false])  LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(transfor(sugar),"(2,20)\n" +
                "(1,10)");
    }


    @Test
    public void selectExceptDistinct() throws IOException {
        String sugar = "fromTable(db1,travelrecord) exceptDistinct fromTable(db1,travelrecord)";
        Schema code = set(Op.EXCEPT_DISTINCT, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")));
        String text = "exceptDistinct(fromTable(db1,travelrecord),fromTable(db1,travelrecord))";

        testText(sugar, text, code,"LogicalMinus(all=[false])  LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(transfor(sugar),"");
    }

    @Test
    public void selectExceptAll() throws IOException {
        String sugar = "fromTable(db1,travelrecord) exceptAll fromTable(db1,travelrecord)";
        Schema code = set(Op.EXCEPT_ALL, Arrays.asList(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord")));
        String text = "exceptAll(fromTable(db1,travelrecord),fromTable(db1,travelrecord))";

        testText(sugar, text, code,"LogicalMinus(all=[true])  LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(transfor("exceptAll( fromTable(db1,travelrecord), table(fields(fieldType(id,int),fieldType(id2,int)),values()))"),"(1,10)\n" +
                "(2,20)");
    }

    @Test
    public void selectFromOrder() throws IOException {
        Schema schema = orderBy(fromTable("db1", "travelrecord"), Arrays.asList(order("id", Direction.ASC), order("user_id", Direction.DESC)));
        String sugar = "fromTable(db1,travelrecord).orderBy(order(id,ASC), order(user_id,DESC))";
        String text = "orderBy(fromTable(db1,travelrecord),order(id,ASC), order(user_id,DESC))";
        testText(sugar, text, schema,"LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])  LogicalTableScan(table=[[db1, travelrecord]])");
    }

    @Test
    public void selectFromLimit() throws IOException {
        Schema schema = limit(fromTable("db1", "travelrecord"), 1, 2);
        String sugar = "fromTable(db1,travelrecord).limit(1,2)";
        String text = "limit(fromTable(db1,travelrecord),1,2)";
        testText(sugar, text, schema,"LogicalSort(offset=[1], fetch=[2])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(schema,"(2,20)");
    }

    @Test
    public void selectFromGroupByKey() throws IOException {
        String sugar = "fromTable(db1,travelrecord).groupBy(keys(groupKey(`id`))))";
        String text = "groupBy(fromTable(db1,travelrecord),keys(groupKey(`id`)))";
        Schema schema = groupBy(fromTable("db1", "travelrecord"), Arrays.asList(groupKey(Arrays.asList(id(("id"))))), Arrays.asList());
        testText(sugar, text, schema,"LogicalAggregate(group=[{0}])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(schema,"(1)\n" +
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

        testSchema(schema,"LogicalAggregate(group=[{0}], agg#0=[AVG(DISTINCT $0) WITHIN GROUP ([1 DESC]) FILTER $2])  LogicalProject(id=[$0], user_id=[$1], $f2=[true])    LogicalTableScan(table=[[db1, travelrecord]])");


        testDump(schema,"(1,1)\n" +
                "(2,2)");
    }


    private Expr literal(Object b) {
        return new Literal(b);
    }

    @Test
    public void selectFromGroupByKeyAvg2() throws IOException {
        String sugar = "fromTable(db1,travelrecord).groupBy(keys(groupKey(`id`),groupKey(`user_id`)),aggregating(avg(`id`),avg(`user_id`))))";
        String text = "groupBy(fromTable(db1,travelrecord),keys(groupKey(`id`),groupKey(`user_id`)),aggregating(avg(`id`),avg(`user_id`)))";
        Schema schema = groupBy(fromTable("db1", "travelrecord"), Arrays.asList(
                groupKey(Arrays.asList(id(("id")))),
                groupKey(Arrays.asList(id(("user_id"))))),
                Arrays.asList(
                        new AggregateCall("avg", Arrays.asList(id(("id")))),
                        new AggregateCall("avg", Arrays.asList(id(("user_id"))))
                ));
        testText(sugar, text, schema);
        testSchema(schema,"LogicalAggregate(group=[{0, 1}], groups=[[{0}, {1}]], agg#0=[AVG($0)], agg#1=[AVG($1)])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(schema,"(1,null,1,10)\n" +
                "(null,20,2,20)\n" +
                "(2,null,2,20)\n" +
                "(null,10,1,10)");
    }

    private Identifier id(String id) {
        return new Identifier(id);
    }

    @Test
    public void selectCastFrom() throws IOException {
        String sugar = "fromTable(db1,travelrecord).map(cast(1,float) as a)";
        String text = "map(fromTable(db1,travelrecord),cast(1,float) as a)";
        Schema schema = map(fromTable("db1", "travelrecord"), Arrays.asList(as(cast(new Literal(1), new Identifier("float")),new Identifier("a"))));
        testText(sugar, text, schema,"LogicalProject(a=[1:FLOAT])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(schema,"(1.0)\n" +
                "(1.0)");
    }

    private Expr cast(Literal literal, Identifier aFloat) {
        return new Expr(Op.CAST, Arrays.asList(literal, aFloat));
    }

    @Test
    public void selectUcaseFrom() throws IOException {
        String sugar = "table(fields(fieldType(id,varchar)),values('A')).map(lower(`id`))";
        String text = "map(table(fields(fieldType(id,varchar)),values('A')),lower(`id`))";
        Schema map = map(table(Arrays.asList(fieldType("id", "varchar")),Arrays.asList("A")), Arrays.asList(new Fun("lower", Arrays.asList(new Identifier("id")))));
        testText(sugar, text, map);
        testSchema(map,"LogicalProject($f0=[LOWER($0)])  LogicalValues(tuples=[[{ 'A' }]])");

        testDump(map,"(a)");
    }

    @Test
    public void filterIn() throws IOException {
        String sugar = "fromTable(db1,travelrecord).filter(`id` = 1)";
        String text = "filter(fromTable(db1,travelrecord),`id` = 1)";
        Schema schema = filter(fromTable("db1", "travelrecord"), eq(new Identifier("id"), new Literal(1)));
        testText(sugar, text, schema);
        testSchema(schema,"LogicalFilter(condition=[=($0, 1)])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(schema,"(1,10)");
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
                        ,eq(new Identifier("id"), new Literal(3))

                )
        );
        testText(sugar, text, schema);
        testSchema(schema,"LogicalFilter(condition=[OR(=($0, 1), =($0, 2), =($0, 3))])  LogicalTableScan(table=[[db1, travelrecord]])");
        testDump(schema,"(1,10)\n" +
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
        testSchema(schema,"LogicalFilter(condition=[=($0, 1)])  LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(schema,"(1,10)");
    }
    @Test
    public void testInnerJoin() throws IOException {
        String sugar = "innerJoin(`id0` = `id`,fromTable(db1,travelrecord).map(`id` as `id0`),fromTable(db1,travelrecord))";
        Schema schema = new JoinSchema(Op.INNER_JOIN,
                eq(id("id0"), id("id"))
                , map(fromTable("db1", "travelrecord"), Arrays.asList( as(new Identifier("id"),new Identifier("id0")))),
                fromTable("db1", "travelrecord")
        );
        testText(sugar, sugar, schema);
        testSchema(schema,"LogicalJoin(condition=[=($1, $0)], joinType=[inner])  LogicalProject(id0=[$0])    LogicalTableScan(table=[[db1, travelrecord]])  LogicalTableScan(table=[[db1, travelrecord]])");
        testDump(schema,"(1,1,10)\n" +
                "(2,2,20)");
    }

    @Test
    public void testCorrelateLeftJoCorrelateSchemain() throws IOException {
        String sugar = "correlateInnerJoin(`t`,table(fields(fieldType(id0,int)),values(1,2,3,4)) , fromTable(`db1`,`travelrecord`).filter(ref(`t`,`id0`) = `id`)))";
        Schema db0 = table(Arrays.asList(fieldType("id0","int")),Arrays.asList(1,2,3,4));
        Schema db1 = filter(fromTable("db1", "travelrecord"), eq(ref("t", "id0"), new Identifier("id")));
        Schema schema = correlate(CORRELATE_INNER_JOIN, "t", db0, db1);
        testText(sugar, sugar, schema);
        testSchema(schema,"LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])  LogicalValues(tuples=[[{ 1 }, { 2 }, { 3 }, { 4 }]])  LogicalFilter(condition=[=($cor0.id0, $0)])    LogicalTableScan(table=[[db1, travelrecord]])");

        testDump(schema,"(1,1,10)\n" +
                "(2,2,20)");
    }

    public static Expr ref(String corName, String fieldName) {
        return new Expr(Op.REF, new Identifier(corName), new Identifier(fieldName));
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


    private GroupItem groupKey(List<Expr> exprList) {
        return new GroupItem(exprList);
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
        return SchemaConvertor.transforSchema( hbtParser.expression());
    }
}
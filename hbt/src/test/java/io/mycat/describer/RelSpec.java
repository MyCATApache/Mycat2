//package io.mycat.parser;
//
//import com.google.common.collect.BiMap;
//import com.google.common.collect.ImmutableList;
//import io.mycat.DesRelNodeHandler;
//import io.mycat.rsqlBuilder.DesBuilder;
//import io.mycat.hbt.BaseQuery;
//import io.mycat.hbt.Op;
//import io.mycat.hbt.QueryOp;
//import io.mycat.hbt.ast.AggregateCall;
//import io.mycat.hbt.ast.base.*;
//import io.mycat.hbt.ast.querySQL.FieldType;
//import io.mycat.hbt.ast.querySQL.SetOpSchema;
//import org.apache.calcite.adapter.java.ReflectiveSchema;
//import org.apache.calcite.plan.RelOptTable;
//import org.apache.calcite.plan.RelOptUtil;
//import org.apache.calcite.rel.RelCollation;
//import org.apache.calcite.rel.RelFieldCollation;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.core.Aggregate;
//import org.apache.calcite.rel.core.CorrelationId;
//import org.apache.calcite.rel.core.JoinRelType;
//import org.apache.calcite.rel.core.SetOp;
//import org.apache.calcite.rel.logical.*;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeField;
//import org.apache.calcite.rex.*;
//import org.apache.calcite.schema.SchemaPlus;
//import org.apache.calcite.sql.SqlAggFunction;
//import org.apache.calcite.sql.SqlKind;
//import org.apache.calcite.sql.SqlOperator;
//import org.apache.calcite.sql.type.SqlTypeName;
//import org.apache.calcite.tools.FrameworkConfig;
//import org.apache.calcite.tools.Frameworks;
//import org.apache.calcite.util.NlsString;
//import org.jetbrains.annotations.NotNull;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.sql.SQLException;
//import java.time.LocalDate;
//import java.time.LocalDateTime;
//import java.time.LocalTime;
//import java.time.format.DateTimeFormatter;
//import java.time.format.DateTimeFormatterBuilder;
//import java.util.*;
//import java.util.concurrent.TimeUnit;
//import java.util.stream.Collectors;
//
//import static io.mycat.DesRelNodeHandler.dump;
//import static io.mycat.DesRelNodeHandler.parse2SyntaxAst;
//import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
//import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
//import static org.apache.calcite.sql.SqlExplainLevel.DIGEST_ATTRIBUTES;
//import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
//
//public class RelSpec extends BaseQuery {
//
//    private List<String> fieldNames;
//    private FrameworkConfig config;
//
//
//
//    @Before
//    public void setUp() {
//        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
//        rootSchema = rootSchema.add("db1", new ReflectiveSchema(new Db1()));
//        this.config = Frameworks.newConfigBuilder()
//                .defaultSchema(rootSchema).build();
//    }
//
//    public RelNode toRelNode(Schema node) {
//        return new QueryOp(DesBuilder.create(config)).complie(node);
//    }
//
//    public RexNode toRexNode(Expr node) {
//        return new QueryOp(DesBuilder.create(config)).toRex(node);
//    }
//
//    private ParseNode getParseNode(String text) {
//        Describer parser = new Describer(text);
//        return parser.expression();
//    }
//
//    @Test
//    public void l() throws IOException {
//
//
//    }
//
//    @Test
//    public void selectWithoutFrom() throws IOException {
//        Schema select;
//        RelNode relNode;
//        select = valuesSchema(fields(fieldType("1", "int")), values());
//        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`int`)),table())", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "int")), values(1, 2, 3, 4, 5));
//        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER 1)], tuples=[[{ 1 }, { 2 }, { 3 }, { 4 }, { 5 }]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`int`)),table(literal(1),literal(2),literal(3),literal(4),literal(5)))", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "int"), fieldType("2", "int")), values(1, 2, 3, 4));
//        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER 1, INTEGER 2)], tuples=[[{ 1, 2 }, { 3, 4 }]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`int`),fieldType(`2`,`int`)),table(literal(1),literal(2),literal(3),literal(4)))", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "varchar"), fieldType("2", "varchar")), values("1", "2", "3", "4"));
//        Assert.assertEquals("LogicalValues(type=[RecordType(VARCHAR 1, VARCHAR 2)], tuples=[[{ '1', '2' }, { '3', '4' }]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`varchar`),fieldType(`2`,`varchar`)),table(literal('1'),literal('2'),literal('3'),literal('4')))", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "float")), values());
//        Assert.assertEquals("LogicalValues(type=[RecordType(FLOAT 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`float`)),table())", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "long")), values());
//        Assert.assertEquals("LogicalValues(type=[RecordType(BIGINT 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`long`)),table())", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "varchar")), values());
//        Assert.assertEquals("LogicalValues(type=[RecordType(VARCHAR 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`varchar`)),table())", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "varbinary")), values());
//        Assert.assertEquals("LogicalValues(type=[RecordType(VARBINARY 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`varbinary`)),table())", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "varbinary")), values(new byte[]{'a'}));
//        Assert.assertEquals("LogicalValues(type=[RecordType(VARBINARY 1)], tuples=[[{ X'61' }]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`varbinary`)),table(literal(X'61')))", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "date")), values());
//        Assert.assertEquals("LogicalValues(type=[RecordType(DATE 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`date`)),table())", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "date")), values(date("2019-11-17")));
//        Assert.assertEquals("LogicalValues(type=[RecordType(DATE 1)], tuples=[[{ 2019-11-17 }]])\n", toString(relNode = toRelNode(select)));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`date`)),table(dateLiteral(2019-11-17)))", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "time")), values(time("00:09:00")));
//        Assert.assertTrue(toString(relNode = toRelNode(select)).contains("TIME"));
//        LocalDateTime now = LocalDateTime.now();
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`time`)),table(timeLiteral(00:09)))", toDSL(relNode));
//
//        select = valuesSchema(fields(fieldType("1", "timestamp")), values(timeStamp(now.toString())));
//        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`time`)),table(timeLiteral(00:09)))", toDSL(relNode));
//
//        Assert.assertTrue(toString(toRelNode(select)).contains("TIMESTAMP"));
//
//
//    }
//
//    static String getFieldName(List<String> fieldNames, int index) {
//        if (fieldNames != null) {
//            return fieldNames.get(index);
//        } else {
//            return "$" + index;
//        }
//    }
//
//    String getFieldName(int index) {
//        return getFieldName(this.fieldNames, index);
//    }
//
//
//    private String toString(RelNode relNode) {
//        return RelOptUtil.toString(relNode, DIGEST_ATTRIBUTES).replaceAll("\r", "");
//    }
//
//    private String toString(RexNode relNode) {
//        return relNode.toString();
//    }
//
//
//    @Test
//    public void selectAllWithoutFrom() throws IOException {
//        RelNode relNode1;
//
//        Schema select = all(valuesSchema(fields(fieldType("1", "int")), values()));
//        Assert.assertEquals("ValuesSchema(table=[], fieldNames=[FieldType(id=1, type=int)])", select.toString());
//
//        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER 1)], tuples=[[]])\n", toString(toRelNode(select)));
//    }
//
//    @Test
//    public void selectAllWithoutFrom2() throws IOException {
//        String text = "all(valuesSchema(fields(fieldType(id,int)),table()))";
//        ParseNode expression = getParseNode(text);
//        Assert.assertEquals(text, expression.toString());
//        String s = getS(expression);
//        Assert.assertEquals("all(valuesSchema(fields(fieldType(id(\"id\"),id(\"int\"))),table()))", s);
//
//
//        Schema select = all(valuesSchema(fields(fieldType("id", "int")), values()));
//        Assert.assertEquals("ValuesSchema(table=[], fieldNames=[FieldType(id=id, type=int)])", select.toString());
//
//        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER id)], tuples=[[]])\n", toString(toRelNode(select)));
//    }
//
//    @Test
//    public void selectWithoutFrom2() throws IOException {
//        Schema anInt = map(valuesSchema(fields(fieldType("1", "int")), values()), eq(id("1"), literal(1)));
//        RelNode relNode1 = toRelNode(anInt);
//        String dsl = toDSL(relNode1);
//        Assert.assertEquals("map(valuesSchema(fields(fieldType(`1`,`int`)),table()),as(eq(`1`,literal(1)),`$f0`))", dsl);
//    }
//
//    @Test
//    public void selectDistinctWithoutFrom2() throws IOException {
//        String text = "distinct(valuesSchema(fields(fieldType(id,int)),table()))";
//        ParseNode expression = getParseNode(text);
//        Assert.assertEquals(text, expression.toString());
//        String s = getS(expression);
//        Assert.assertEquals("distinct(valuesSchema(fields(fieldType(id(\"id\"),id(\"int\"))),table()))", s);
//
//        Schema select = distinct(valuesSchema(fields(fieldType("id", "int")), values()));
//        Assert.assertEquals("DistinctSchema(schema=ValuesSchema(table=[], fieldNames=[FieldType(id=id, type=int)]))", select.toString());
//    }
//
//    @Test
//    public void selectDistinctWithoutFrom() throws IOException {
//        Schema select = distinct(valuesSchema(fields(fieldType("1", "int")), values(2, 2)));
//        Assert.assertEquals("DistinctSchema(schema=ValuesSchema(table=[Literal(value=2), Literal(value=2)], fieldNames=[FieldType(id=1, type=int)]))", select.toString());
//        RelNode relNode = toRelNode(select);
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}])\n" +
//                "  LogicalValues(type=[RecordType(INTEGER 1)], tuples=[[{ 2 }, { 2 }]])\n", toString(relNode));
//        Assert.assertEquals("(2)\n", dump(relNode));
//
//        String dsl = toDSL(relNode);
//        Assert.assertEquals("group(valuesSchema(fields(fieldType(`1`,`int`)),table(literal(2),literal(2))),keys(regular(`1`)),aggregating())", dsl);
//    }
//
//    @Test
//    public void selectProjectItemWithoutFrom2() throws IOException {
//        String text = "rename(valuesSchema(fields(fieldType(id,int),fieldType(id2,int)),table()),id3,id4)";
//        ParseNode expression = getParseNode(text);
//        Assert.assertEquals(text, expression.toString());
//        String s = getS(expression);
//        Assert.assertEquals("rename(valuesSchema(fields(fieldType(id(\"id\"),id(\"int\")),fieldType(id(\"id2\"),id(\"int\"))),table()),id(\"id3\"),id(\"id4\"))", s);
//
//        Schema select = projectNamed(valuesSchema(fields(fieldType("id", "int"), fieldType("id2", "varchar")), values()), "id3", "id4");
//        Assert.assertEquals("ProjectSchema(schema=ValuesSchema(table=[], fieldNames=[FieldType(id=id, type=int), FieldType(id=id2, type=varchar)]), columnNames=[id3, id4], fieldSchemaList=[FieldType(id=id, type=int), FieldType(id=id2, type=varchar)])", select.toString());
//    }
//
//    @Test
//    public void fromTable() throws IOException {
//        String text = "fromTable(db1,travelrecord)";
//        RelNode relNode;
//        ParseNode expression = getParseNode(text);
//        Assert.assertEquals(text, expression.toString());
//        String s = getS(expression);
//        Assert.assertEquals("fromTable(id(\"db1\"),id(\"travelrecord\"))", s);
//
//        Assert.assertEquals("FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])", fromTable(id("db1"), id("travelrecord")).toString());
//
//        Schema select = fromTable("db1", "travelrecord");
//        Assert.assertEquals("FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])", select.toString());
//
//        Assert.assertEquals("LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(select)));
//
//        String dsl = toDSL(relNode);
//        Assert.assertEquals("fromTable(`db1`,`travelrecord`)", dsl);
//    }
//
//    @Test
//    public void selectProjectItemWithoutFrom() throws IOException {
//        RelNode relNode;
//        Schema select = projectNamed(valuesSchema(fields(fieldType("1", "int"), fieldType("2", "varchar")), values()), "2", "1");
//        Assert.assertEquals("ProjectSchema(schema=ValuesSchema(table=[], fieldNames=[FieldType(id=1, type=int), FieldType(id=2, type=varchar)]), columnNames=[2, 1], fieldSchemaList=[FieldType(id=1, type=int), FieldType(id=2, type=varchar)])", select.toString());
//
//        Assert.assertEquals("LogicalProject(2=[$0], 1=[$1])\n" +
//                "  LogicalValues(type=[RecordType(INTEGER 1, VARCHAR 2)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
//
//        String dsl = toDSL(relNode);
//        Assert.assertEquals("map(valuesSchema(fields(fieldType(`1`,`int`),fieldType(`2`,`varchar`)),table()),as(`1`,`2`),as(`2`,`1`))", dsl);
//    }
//
//    @Test
//    public void selectUnionAll() throws IOException {
//        RelNode relNode;
//        Schema select = unionAll(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord"));
//        Assert.assertEquals("SetOpSchema(op=UNION_ALL,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());
//
//        String text = "fromTable(db1,travelrecord) unionAll  fromTable(\"db1\", \"travelrecord\")";
//        String s = getS(parse2SyntaxAst(text));
//        Assert.assertEquals("unionAll(fromTable(id(\"db1\"),id(\"travelrecord\")),fromTable(id(\"db1\"),id(\"travelrecord\")))", s);
//
//        Assert.assertEquals("LogicalUnion(all=[true])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(select)));
//
//        String dsl = toDSL(relNode = toRelNode(select));
//        Assert.assertEquals("unionAll(fromTable(`db1`,`travelrecord`),fromTable(`db1`,`travelrecord`))", dsl);
//    }
//
//    @Test
//    public void selectUnionDistinct() throws IOException {
//        RelNode relNode;
//        Schema select = unionDistinct(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord"));
//        Assert.assertEquals("SetOpSchema(op=UNION_DISTINCT,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());
//
//        String text = "fromTable(db1,travelrecord) unionDistinct  fromTable(\"db1\", \"travelrecord\")";
//        Assert.assertEquals("unionDistinct(fromTable(id(\"db1\"),id(\"travelrecord\")),fromTable(id(\"db1\"),id(\"travelrecord\")))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalUnion(all=[false])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(select)));
//
//        String dsl = toDSL(relNode = toRelNode(select));
//        Assert.assertEquals("unionDistinct(fromTable(`db1`,`travelrecord`),fromTable(`db1`,`travelrecord`))", dsl);
//
//
//    }
//
//    private String getS(ParseNode parseNode) {
//        return DesRelNodeHandler.syntaxAstToFlatSyntaxAstText(parseNode);
//    }
//
//    @Test
//    public void selectProjectFrom() throws IOException {
//        String text = "fromTable(db1,travelrecord).rename(id)";
//        RelNode relNode;
//        String s = getS(parse2SyntaxAst(text));
//        Assert.assertEquals("rename(fromTable(id(\"db1\"),id(\"travelrecord\")),id(\"id\"))", s);
//
//        Schema select = projectNamed(fromTable("db1", "travelrecord"), "1", "2");
//        Assert.assertEquals("ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), columnNames=[1, 2], fieldSchemaList=[])", select.toString());
//
//        String dsl = toDSL(relNode = toRelNode(select));
//        Assert.assertEquals("map(fromTable(`db1`,`travelrecord`),as(`id`,`1`),as(`user_id`,`2`))", dsl);
//
//        select = projectNamed(fromTable("db1", "travelrecord"));
//        Assert.assertEquals("ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), columnNames=[], fieldSchemaList=[])", select.toString());
//
//        dsl = toDSL(relNode = toRelNode(select));
//        Assert.assertEquals("fromTable(`db1`,`travelrecord`)", dsl);
//    }
//
//    @Test
//    public void selectExceptDistinct() throws IOException {
//        RelNode relNode;
//        Schema select = exceptDistinct(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord"));
//        Assert.assertEquals("SetOpSchema(op=EXCEPT_DISTINCT,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());
//
//        String text = "fromTable(db1,travelrecord) exceptDistinct  fromTable(db1,travelrecord)";
//        Assert.assertEquals("exceptDistinct(fromTable(id(\"db1\"),id(\"travelrecord\")),fromTable(id(\"db1\"),id(\"travelrecord\")))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalMinus(all=[false])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(select)));
//
//        String dsl = toDSL(relNode = toRelNode(select));
//        Assert.assertEquals("exceptDistinct(fromTable(`db1`,`travelrecord`),fromTable(`db1`,`travelrecord`))", dsl);
//    }
//
//    @Test
//    public void selectExceptAll() throws IOException {
//        RelNode relNode;
//        Schema select = exceptAll(fromTable("db1", "travelrecord"), fromTable("db1", "travelrecord"));
//        Assert.assertEquals("SetOpSchema(op=EXCEPT_ALL,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());
//
//        String text = "fromTable(db1,travelrecord) exceptAll  fromTable(\"db1\", \"travelrecord\")";
//        Assert.assertEquals("exceptAll(fromTable(id(\"db1\"),id(\"travelrecord\")),fromTable(id(\"db1\"),id(\"travelrecord\")))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalMinus(all=[true])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(select)));
//
//        String dsl = toDSL(relNode = toRelNode(select));
//        Assert.assertEquals("exceptAll(fromTable(`db1`,`travelrecord`),fromTable(`db1`,`travelrecord`))", dsl);
//    }
//
//    @Test
//    public void selectFromOrder() throws IOException {
//        RelNode relNode;
//        Schema schema = orderBy(fromTable("db1", "travelrecord"), order("id", "ASC"), order("user_id", "DESC"));
//        Assert.assertEquals("OrderSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), orders=[OrderItem(columnName=Identifier(value=id), direction=ASC), OrderItem(columnName=Identifier(value=user_id), direction=DESC)])", schema.toString());
//
//        String text = "orderBy(fromTable(db1,travelrecord),order(id,ASC), order(user_id,DESC))";
//        Assert.assertEquals("orderBy(fromTable(id(\"db1\"),id(\"travelrecord\")),order(id(\"id\"),id(\"asc\")),order(id(\"user_id\"),id(\"desc\")))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
//
//        String dsl = toDSL(relNode = toRelNode(schema));
//        Assert.assertEquals("orderBy(fromTable(`db1`,`travelrecord`),order(`id`,`ASC`),order(`user_id`,`DESC`))", dsl);
//    }
//
//    @Test
//    public void selectFromLimit() throws IOException {
//        RelNode relNode;
//        Schema schema = limit(fromTable("db1", "travelrecord"), 1, 1000);
//        Assert.assertEquals("LimitSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), offset=Literal(value=1), limit=Literal(value=1000))", schema.toString());
//
//        String text = "limit(fromTable(db1,travelrecord),order(id,ASC), order(user_id,DESC),1,1000)";
//        Assert.assertEquals("limit(fromTable(id(\"db1\"),id(\"travelrecord\")),order(id(\"id\"),id(\"asc\")),order(id(\"user_id\"),id(\"desc\")),literal(1),literal(1000))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalSort(offset=[1], fetch=[1000])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
//
//        String dsl = toDSL(relNode = toRelNode(schema));
//        Assert.assertEquals("limit(fromTable(`db1`,`travelrecord`),1,1000)", dsl);
//    }
//
//    @Test
//    public void selectFromGroupByKey() throws IOException {
//        RelNode relNode;
//        Schema schema = group(fromTable("db1", "travelrecord"), keys(regular(id("id"))));
//        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[])", schema.toString());
//
//        String text = "group(fromTable(db1,travelrecord),keys(regular(id)))";
//        Assert.assertEquals("group(fromTable(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
//
//        String dsl = toDSL(relNode = toRelNode(schema));
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating())", dsl);
//    }
//
//    @Test
//    public void selectFromGroupByKeyCount() throws IOException {
//        RelNode relNode;
//        Schema schema = group(fromTable("db1", "travelrecord"), keys(regular(id("id"))), aggregating(count("id")));
//        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='count', alias='count(id)', operands=[Identifier(value=id)]])", schema.toString());
//
//        String text = "group(fromTable(db1,travelrecord),keys(regular(id)), aggregating(count(id)))";
//        Assert.assertEquals("group(fromTable(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(count(id(\"id\"))))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));
//
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`count`,`id`),`count(id)`)))", toDSL(relNode));
//    }
//
//    @Test
//    public void selectFromGroupByKeyCountStar() throws IOException {
//        RelNode relNode;
//        Schema schema = group(fromTable("db1", "travelrecord"), keys(regular(id("id"))), aggregating(count()));
//        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='count', alias='count()', operands=[]])", schema.toString());
//
//        String text = "group(fromTable(db1,travelrecord),keys(regular(id)), aggregating(countStar()))";
//        Assert.assertEquals("group(fromTable(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(countStar()))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], count()=[COUNT()])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));
//
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`count`),`count()`)))", toDSL(relNode));
//    }
//
//
//    @Test
//    public void selectFromGroupByKeyCountDistinct() throws IOException {
//        RelNode relNode;
//        Schema schema = group(fromTable("db1", "travelrecord"), keys(regular(id("id"))), aggregating(distinct(count("id"))));
//        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='count', distinct=true, alias='count(id)', operands=[Identifier(value=id)]])", schema.toString());
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(distinct(as(call(`count`,`id`),`count(id)`))))", toDSL(relNode = toRelNode(schema)));
//
//        String text = "group(fromTable(db1,travelrecord),keys(regular(id)), aggregating(count(id).distinct()))";
//        Assert.assertEquals("group(fromTable(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(distinct(count(id(\"id\")))))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT(DISTINCT $0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(fromTable("db1", "travelrecord"), keys(regular(id("id"))), aggregating(all(distinct(count("id"))))))));
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`count`,`id`),`count(id)`)))", toDSL(relNode));
//
//
////        Assert.assertEquals("LogicalAggregate(group=[{0}], asName=[COUNT($0)])\n" +
////                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(fromTable("db1", "travelrecord"), keys(regular(id("id"))),
////                aggregating((as(count("id"), "asName")))))));
////        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`count`,`id`),`asName`)))", toDSL(relNode));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(fromTable("db1", "travelrecord"), keys(regular(id("id"))),
//                aggregating((approximate(count("id"))))))));
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(approximate(as(call(`count`,`id`),`count(id)`))))", toDSL(relNode));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(fromTable("db1", "travelrecord"), keys(regular(id("id"))),
//                aggregating((exact(count("id"))))))));
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`count`,`id`),`count(id)`)))", toDSL(relNode));
//
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0) FILTER $2])\n" +
//                "  LogicalProject(id=[$0], user_id=[$1], $f2=[=($0, 1)])\n" +
//                "    LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(fromTable("db1", "travelrecord"), keys(regular(id("id"))),
//                aggregating((filter(count("id"), eq(id("id"), literal(1)))))))));
//        Assert.assertEquals("group(map(fromTable(`db1`,`travelrecord`),as(`id`,`id`),as(`user_id`,`user_id`),as(eq(`id`,literal(1)),`$f2`)),keys(regular(`id`)),aggregating(filter(as(call(`count`,`id`),`count(id)`),`$f2`)))", toDSL(relNode));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(fromTable("db1", "travelrecord"), keys(regular(id("id"))),
//                aggregating((ignoreNulls(count("id"))))))));
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(ignoreNulls(as(call(`count`,`id`),`count(id)`))))", toDSL(relNode));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0) WITHIN GROUP ([0 DESC])])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(fromTable("db1", "travelrecord"), keys(regular(id("id"))),
//                aggregating((orderBy(count("id"), order("id", "DESC"))))))));
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(sort(as(call(`count`,`id`),`count(id)`),order(id,desc))))", toDSL(relNode));
//
//    }
//
//    @Test
//    public void selectFromGroupByKeyFirst() throws IOException {
//        RelNode relNode;
//        Schema schema = group(fromTable("db1", "travelrecord"), keys(regular(id("id"))), aggregating(first("id")));
//        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='first', alias='first(id)', operands=[Identifier(value=id)]])", schema.toString());
//
//        String text = "group(fromTable(db1,travelrecord),keys(regular(id)), aggregating(first(id)))";
//        Assert.assertEquals("group(fromTable(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(first(id(\"id\"))))", getS(parse2SyntaxAst(text)));
//
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], first(id)=[FIRST_VALUE($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`first`,`id`),`first(id)`)))", toDSL(relNode));
//
//    }
//
//    @Test
//    public void selectFromGroupByKeyLast() throws IOException {
//        Schema schema = group(fromTable("db1", "travelrecord"), keys(regular(id("id"))), aggregating(last("id")));
//        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='last', alias='last(id)', operands=[Identifier(value=id)]])", schema.toString());
//
//        String text = "group(fromTable(db1,travelrecord),keys(regular(id)), aggregating(last(id)))";
//        Assert.assertEquals("group(fromTable(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(last(id(\"id\"))))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], last(id)=[LAST_VALUE($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
//
//
//    }
//
//    @Test
//    public void selectFromGroupByKeyMax() throws IOException {
//        Schema schema = group(fromTable("db1", "travelrecord"), keys(regular(id("id"))), aggregating(max("id")));
//        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='max', alias='max(id)', operands=[Identifier(value=id)]])", schema.toString());
//
//        String text = "group(fromTable(db1,travelrecord),keys(regular(id)), aggregating(max(id)))";
//        Assert.assertEquals("group(fromTable(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(max(id(\"id\"))))", getS(parse2SyntaxAst(text)));
//
//        String text2 = "fromTable(db1,travelrecord).group(keys(regular(id)),aggregating(max(id)))";
//        Assert.assertEquals("group(fromTable(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(max(id(\"id\"))))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], max(id)=[MAX($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
//    }
//
//    @Test
//    public void selectFromGroupByKeyMin() throws IOException {
//        Schema schema = group(fromTable("db1", "travelrecord"), keys(regular(id("id"))), aggregating(min("id")));
//        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='min', alias='min(id)', operands=[Identifier(value=id)]])", schema.toString());
//
//        String text2 = "fromTable(db1,travelrecord).group(keys(regular(id)),aggregating(min(id)))";
//        Assert.assertEquals("group(fromTable(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(min(id(\"id\"))))", getS(parse2SyntaxAst(text2)));
//
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], min(id)=[MIN($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
//    }
//
//    @Test
//    public void selectUpperFrom() throws IOException {
//        Schema schema = map(fromTable("db1", "travelrecord"), upper("id"));
//        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), convertRexNode=[upper(Identifier(value=id))])", schema.toString());
//
//        String text2 = "fromTable(db1,travelrecord).map(upper(id))";
//        Assert.assertEquals("map(fromTable(id(\"db1\"),id(\"travelrecord\")),upper(id(\"id\")))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("LogicalProject($f0=[UPPER($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
//    }
//
//    @Test
//    public void selectLowerFrom() throws IOException {
//        Schema schema = map(fromTable("db1", "travelrecord"), lower("id"));
//        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), convertRexNode=[lower(Identifier(value=id))])", schema.toString());
//
//        String text2 = "fromTable(db1,travelrecord).map(lower(id))";
//        Assert.assertEquals("map(fromTable(id(\"db1\"),id(\"travelrecord\")),lower(id(\"id\")))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("LogicalProject($f0=[LOWER($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
//    }
//
//    @Test
//    public void selectMidFrom() throws IOException {
//        Schema schema = map(fromTable("db1", "travelrecord"), mid("id", 1));
//        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), convertRexNode=[mid(Identifier(value=id),Literal(value=1))])", schema.toString());
//
//        String text2 = "fromTable(db1,travelrecord).map(mid(id))";
//        Assert.assertEquals("map(fromTable(id(\"db1\"),id(\"travelrecord\")),mid(id(\"id\")))", getS(parse2SyntaxAst(text2)));
//    }
//
//    @Test
//    public void selectLenFrom() throws IOException {
//        Schema schema = map(fromTable("db1", "travelrecord"), len("id"));
//        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), convertRexNode=[len(Identifier(value=id))])", schema.toString());
//
//        String text2 = "fromTable(db1,travelrecord).map(len(id))";
//        Assert.assertEquals("map(fromTable(id(\"db1\"),id(\"travelrecord\")),len(id(\"id\")))", getS(parse2SyntaxAst(text2)));
//    }
//
//    @Test
//    public void selectRoundFrom() throws IOException {
//        Schema schema = map(fromTable("db1", "travelrecord"), round("id", 2));
//        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), convertRexNode=[round(Identifier(value=id),Literal(value=2))])", schema.toString());
//
//        String text2 = "fromTable(db1,travelrecord).map(round(id,2))";
//        Assert.assertEquals("map(fromTable(id(\"db1\"),id(\"travelrecord\")),round(id(\"id\"),literal(2)))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("LogicalProject($f0=[ROUND($0, 2)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
//    }
//
//    @Test
//    public void selectNowFrom() throws IOException {
//        Schema schema = map(fromTable("db1", "travelrecord"), now());
//        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), convertRexNode=[now()])", schema.toString());
//
//        String text2 = "fromTable(db1,travelrecord).map(now())";
//        Assert.assertEquals("map(fromTable(id(\"db1\"),id(\"travelrecord\")),now())", getS(parse2SyntaxAst(text2)));
//
//
//    }
//
//    @Test
//    public void selectFormatFrom() throws IOException {
//        Schema schema = map(fromTable("db1", "travelrecord"), format(now(), "YYYY-MM-DD"));
//        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), convertRexNode=[format(now(),Literal(value=YYYY-MM-DD))])", schema.toString());
//
//        String text2 = "fromTable(db1,travelrecord).map(format(now(),'YYYY-MM-DD'))";
//        Assert.assertEquals("map(fromTable(id(\"db1\"),id(\"travelrecord\")),format(now(),literal(\"YYYY-MM-DD\")))", getS(parse2SyntaxAst(text2)));
//
//    }
//
//    @Test
//    public void filterIn() throws IOException {
//        RelNode relNode;
//        Schema schema = filter(fromTable("db1", "travelrecord"), in("id", 1, 2));
//        Assert.assertEquals("FilterSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), exprs=[or(eq(Identifier(value=id),Literal(value=1)),eq(Identifier(value=id),Literal(value=2)))])", schema.toString());
//
//        String text2 = "fromTable(db1,travelrecord).filter(in(id,1,2))";
//        Assert.assertEquals("filter(fromTable(id(\"db1\"),id(\"travelrecord\")),in(id(\"id\"),literal(1),literal(2)))", getS(parse2SyntaxAst(text2)));
//
//
//        Assert.assertEquals("LogicalFilter(condition=[OR(=($0, 1), =($0, 2))])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));
//        Assert.assertEquals("filter(fromTable(`db1`,`travelrecord`),or(eq(`id`,literal(1)),eq(`id`,literal(2))))", toDSL(relNode));
//    }
//
//
//    @Test
//    public void filterBetween() throws IOException {
//        RelNode relNode;
//        Schema schema = filter(fromTable("db1", "travelrecord"), between("id", 1, 4));
//        Assert.assertEquals("FilterSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), exprs=[and(gte(Identifier(value=id),Literal(value=1)),lte(Identifier(value=id),Literal(value=4)))])", schema.toString());
//
//
//        String text2 = "fromTable(db1,travelrecord).filter(between(id,1,4))";
//        Assert.assertEquals("filter(fromTable(id(\"db1\"),id(\"travelrecord\")),between(id(\"id\"),literal(1),literal(4)))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("LogicalFilter(condition=[AND(>=($0, 1), <=($0, 4))])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));
//        Assert.assertEquals("filter(fromTable(`db1`,`travelrecord`),and(gte(`id`,literal(1)),lte(`id`,literal(4))))", toDSL(relNode));
//    }
//
//    @Test
//    public void testIsnull() throws IOException {
//
//        Assert.assertEquals("isnull(Identifier(value=id))", isnull(id("id")).toString());
//
//        String text2 = "isnull(id)";
//        Assert.assertEquals("isnull(id(\"id\"))", getS(parse2SyntaxAst(text2)));
//
//        Expr convertRexNode = isnull(literal(1));
//        Assert.assertEquals("IS NULL(1)", toString(toRexNode(convertRexNode)));
//    }
//
//    @Test
//    public void testIfnull() throws IOException {
//        Assert.assertEquals("ifnull(Identifier(value=id),Literal(value=default))", ifnull("id", "default").toString());
//
//        String text2 = "ifnull(id)";
//        Assert.assertEquals("ifnull(id(\"id\"))", getS(parse2SyntaxAst(text2)));
//
//    }
//
//    @Test
//    public void testNullif() throws IOException {
//        Expr convertRexNode = nullif("id", "default");
//        Assert.assertEquals("nullif(Identifier(value=id),Literal(value=default))", convertRexNode.toString());
//
//        String text2 = "nullif(id)";
//        Assert.assertEquals("nullif(id(\"id\"))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("NULLIF(null:NULL, null:NULL)", toString(toRexNode(nullif(literal(null), literal(null)))));
//    }
//
//    @Test
//    public void testIsNotNull() throws IOException {
//        Expr convertRexNode = isnotnull("id");
//        Assert.assertEquals("isnotnull(Identifier(value=id))", convertRexNode.toString());
//
//        String text2 = "isnotnull(id)";
//        Assert.assertEquals("isnotnull(id(\"id\"))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("IS NOT NULL(null:NULL)", toString(toRexNode(isnotnull(literal(null)))));
//
//    }
//
//    @Test
//    public void testInteger() throws IOException {
//        Expr convertRexNode = literal(1);
//        RexNode rexNode;
//        Assert.assertEquals("Literal(value=1)", convertRexNode.toString());
//
//        String text2 = "1";
//        Assert.assertEquals("literal(1)", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("1", toString(rexNode = toRexNode(literal(Integer.valueOf(1)))));
//
//        Expr expr1 = convertRexNode(rexNode);
//        Assert.assertEquals("Literal(value=1)", expr1.toString());
//    }
//
//    @Test
//    public void testLong() throws IOException {
//        Expr convertRexNode = literal(1L);
//        RexNode rexNode;
//
//        Assert.assertEquals("Literal(value=1)", convertRexNode.toString());
//        String text2 = "1";
//        Assert.assertEquals("literal(1)", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("1", toString(rexNode = toRexNode(literal(Long.valueOf(1)))));
//
//        Expr expr1 = convertRexNode(rexNode);
//        Assert.assertEquals("Literal(value=1)", expr1.toString());
//    }
//
//    @Test
//    public void testFloat() throws IOException {
//        Expr convertRexNode = literal(Float.MAX_VALUE);
//        RexNode rexNode;
//
//        Assert.assertEquals("Literal(value=3.4028234663852886E+38)", convertRexNode.toString());
//        String text2 = String.valueOf(Float.MAX_VALUE);
//        Assert.assertEquals("literal(3.4028235E+38)", getS(parse2SyntaxAst(text2)));
//        Assert.assertEquals("1.0:DECIMAL(2, 1)", toString(rexNode = toRexNode(literal(Float.valueOf(1)))));
//
//        Expr expr1 = convertRexNode(rexNode);
//        Assert.assertEquals("Literal(value=1.0)", expr1.toString());
//    }
//
//    @Test
//    public void testId() throws IOException {
//        Expr convertRexNode = id("id");
//        Assert.assertEquals("Identifier(value=id)", convertRexNode.toString());
//
//        String text2 = "id";
//        Assert.assertEquals("id(\"id\")", getS(parse2SyntaxAst(text2)));
//    }
//
//    @Test
//    public void testString() throws IOException {
//        Expr convertRexNode = literal("str");
//        Assert.assertEquals("Literal(value=str)", convertRexNode.toString());
//
//        String text2 = "'str'";
//        Assert.assertEquals("literal(\"str\")", getS(parse2SyntaxAst(text2)));
//
//        RexNode rexNode = toRexNode(literal("str"));
//        Assert.assertEquals("'str'", toString(rexNode));
//
//        Expr expr1 = convertRexNode(rexNode);
//        Assert.assertEquals("Literal(value=str)", expr1.toString());
//    }
//
//    @Test
//    public void testTime() throws IOException {
//        Expr convertRexNode = literal(time("00:09:00"));
//        Assert.assertEquals("Literal(value=00:09)", convertRexNode.toString());
//
//        String text2 = "time('00:09:00')";
//        Assert.assertEquals("time(literal(\"00:09:00\"))", getS(parse2SyntaxAst(text2)));
//
//        RexNode rexNode = toRexNode(timeLiteral("00:09:00"));
//        Assert.assertEquals("00:09:00", toString(rexNode));
//
//        Expr expr1 = convertRexNode(rexNode);
//        Assert.assertEquals("Literal(value=00:09)", expr1.toString());
//    }
//
//    @Test
//    public void testDate() throws IOException {
//        Expr convertRexNode = literal(date("2019-11-17"));
//        Assert.assertEquals("Literal(value=2019-11-17)", convertRexNode.toString());
//
//        String text2 = "date('2019-11-17')";
//        Assert.assertEquals("date(literal(\"2019-11-17\"))", getS(parse2SyntaxAst(text2)));
//        RexNode rexNode = toRexNode(dateLiteral("2019-11-17"));
//        Assert.assertEquals("2019-11-17", toString(rexNode));
//
//        Expr expr1 = convertRexNode(rexNode);
//        Assert.assertEquals("Literal(value=2019-11-17)", expr1.toString());
//    }
//
//    @Test
//    public void testTimeStamp() throws IOException {
//
//
//        LocalDateTime now = LocalDateTime.parse("2019-11-22T12:12:03");
//        String text = now.toString();
//        Expr convertRexNode = literal(timeStamp(text));
//        Assert.assertEquals("Literal(value=" + text +
//                ")", convertRexNode.toString());
//
//        String text2 = "timeStamp('" +
//                text +
//                "')";
//        Assert.assertEquals("timeStamp(literal(\"" +
//                text +
//                "\"))", getS(parse2SyntaxAst(text2)));
//        RexNode rexNode = toRexNode(timeStampLiteral(text));
//        Assert.assertEquals("2019-11-22 12:12:03", toString(rexNode));
//
//        Expr expr1 = convertRexNode(rexNode);
//        Assert.assertEquals("Literal(value=" +
//                text +
//                ")", expr1.toString());
//    }
//
//    @Test
//    public void testMinus() throws IOException {
//        Expr convertRexNode = minus(id("id"), literal(1));
//        RexNode rexNode;
//        Assert.assertEquals("minus(Identifier(value=id),Literal(value=1))", convertRexNode.toString());
//
//        String text2 = "id-1";
//        Assert.assertEquals("minus(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("-(1, 1)", toString(rexNode = toRexNode(minus(literal(1), literal(1)))));
//        String dsl = toDSL(rexNode);
//    }
//
//    @Test
//    public void testEqual() throws IOException {
//        Expr convertRexNode = eq(id("id"), literal(1));
//        Assert.assertEquals("eq(Identifier(value=id),Literal(value=1))", convertRexNode.toString());
//
//        String text2 = "id=1";
//        Assert.assertEquals("eq(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("=(1, 1)", toString(toRexNode(eq(literal(1), literal(1)))));
//    }
//
//    @Test
//    public void testAnd() throws IOException {
//        Expr convertRexNode = and(literal(1), literal(1));
//        Assert.assertEquals("and(Literal(value=1),Literal(value=1))", convertRexNode.toString());
//
//        String text2 = "1 and 1";
//        Assert.assertEquals("and(literal(1),literal(1))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("AND(1, 1)", toString(toRexNode(convertRexNode)));
//    }
//
//    @Test
//    public void testor() throws IOException {
//        Expr convertRexNode = or(literal(1), literal(1));
//        Assert.assertEquals("or(Literal(value=1),Literal(value=1))", convertRexNode.toString());
//
//        String text2 = "1 or 1";
//        Assert.assertEquals("or(literal(1),literal(1))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("OR(1, 1)", toString(toRexNode(convertRexNode)));
//    }
//
//    @Test
//    public void testEqualOr() throws IOException {
//        String text2 = "1 = 2 or 1 = 1";
//        Assert.assertEquals("or(eq(literal(1),literal(2)),eq(literal(1),literal(1)))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("OR(=(1, 2), =(1, 1))", toString(toRexNode(or(eq(literal(1), literal(2)), eq(literal(1), literal(1))))));
//    }
//
//    @Test
//    public void testNot() throws IOException {
//        Expr convertRexNode = not(literal(1));
//        Assert.assertEquals("not(Literal(value=1))", convertRexNode.toString());
//
//        String text2 = "not(1 = 1)";
//        Assert.assertEquals("not(eq(literal(1),literal(1)))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("NOT(=(1, 1))", toString(toRexNode(not(eq(literal(1), literal(1))))));
//
//    }
//
//    @Test
//    public void testNotEqual() throws IOException {
//        Expr convertRexNode = ne(id("id"), literal(1));
//        Assert.assertEquals("ne(Identifier(value=id),Literal(value=1))", convertRexNode.toString());
//
//        String text2 = "id <> 1";
//        Assert.assertEquals("ne(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("NOT(<>(1, 1))", toString(toRexNode(not(ne(literal(1), literal(1))))));
//    }
//
//    @Test
//    public void testGreaterThan() throws IOException {
//        Expr convertRexNode = gt(id("id"), literal(1));
//        Assert.assertEquals("gt(Identifier(value=id),Literal(value=1))", convertRexNode.toString());
//
//        String text2 = "id > 1";
//        Assert.assertEquals("gt(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals(">(1, 1)", toString(toRexNode((gt(literal(1), literal(1))))));
//    }
//
//    @Test
//    public void testGreaterThanEqual() throws IOException {
//        Expr convertRexNode = gte(id("id"), literal(true));
//        Assert.assertEquals("gte(Identifier(value=id),Literal(value=true))", convertRexNode.toString());
//
//        String text2 = "id >= 1";
//        Assert.assertEquals("gte(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals(">=(1, 1)", toString(toRexNode((gte(literal(1), literal(1))))));
//    }
//
//    @Test
//    public void testLessThan() throws IOException {
//        Expr convertRexNode = lt(id("id"), literal(true));
//        Assert.assertEquals("lt(Identifier(value=id),Literal(value=true))", convertRexNode.toString());
//
//        String text2 = "id < 1";
//        Assert.assertEquals("lt(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("<(1, 1)", toString(toRexNode((lt(literal(1), literal(1))))));
//    }
//
//    @Test
//    public void testLessThanEqual() throws IOException {
//        Expr convertRexNode = lte(id("id"), literal(true));
//        Assert.assertEquals("lte(Identifier(value=id),Literal(value=true))", convertRexNode.toString());
//
//        String text2 = "id <= 1";
//        Assert.assertEquals("lte(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("<=(1, 1)", toString(toRexNode((lte(literal(1), literal(1))))));
//    }
//
//    @Test
//    public void testAsColumnName() throws IOException {
//        RelNode relNode;
//        Expr convertRexNode = as(literal(1), id("column"));
//        Assert.assertEquals("asColumnName(Literal(value=1),Identifier(value=column))", convertRexNode.toString());
//
//        String text2 = "1 as column";
//        Assert.assertEquals("as(literal(1),id(\"column\"))", getS(parse2SyntaxAst(text2)));
//
//        Schema map = map(fromTable("db1", "travelrecord"), as(id("user_id"), id("id")), as(literal(1), id("column")));
//        Assert.assertEquals("LogicalProject(id=[$1], column=[1])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(map)));
//
//        Assert.assertEquals("map(fromTable(`db1`,`travelrecord`),as(`user_id`,`id`),as(literal(1),`column`))", toDSL(relNode));
//    }
//
//    @Test
//    public void testCast() throws IOException {
//        RexNode rexNode;
//        Expr convertRexNode = cast(literal(1), id("float"));
//        Assert.assertEquals("cast(Literal(value=1),Identifier(value=float))", convertRexNode.toString());
//
//        String text2 = "cast(1,float)";
//        Assert.assertEquals("cast(literal(1),id(\"float\"))", getS(parse2SyntaxAst(text2)));
//
//        Assert.assertEquals("1:FLOAT", toString(rexNode = toRexNode(convertRexNode)));
//        Assert.assertEquals("Literal(value=1)", toDSL(rexNode));
//
//        Assert.assertEquals("map(fromTable(`db1`,`travelrecord`),as(cast(`id`,`float`),`id`))", toDSL(toRelNode(map((fromTable("db1", "travelrecord")), cast(id("id"), id("float"))))));
//    }
//
//    @Test
//    public void testInnerJoin() throws IOException, SQLException {
//
//        Schema schema = innerJoin(eq(id("id0"), id("id")),
//                fromTable("db1", "travelrecord"),
//                projectNamed(fromTable("db1", "travelrecord2"), "id0", "user_id0"));
//        Assert.assertEquals("JoinSchema(type=INNER_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(Identifier(value=id0),Identifier(value=id)))", schema.toString());
//
//        String text2 = "innerJoin(table.id = table2.id , fromTable(db1,travelrecord),fromTable(db1,travelrecord2))";
//        Assert.assertEquals("innerJoin(eq(dot(id(\"table\"),id(\"id\")),dot(id(\"table2\"),id(\"id\"))),fromTable(id(\"db1\"),id(\"travelrecord\")),fromTable(id(\"db1\"),id(\"travelrecord2\")))",
//                getS(parse2SyntaxAst(text2)));
//
//        RelNode relNode = toRelNode(schema);
//        Assert.assertEquals("LogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalProject(id0=[$0], user_id0=[$1])\n" +
//                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
//        dump(relNode);
//
//        Assert.assertEquals("join(innerJoin,eq(`id`,`id0`),fromTable(`db1`,`travelrecord`),map(fromTable(`db1`,`travelrecord2`),as(`id`,`id0`),as(`user_id`,`user_id0`)))",
//                toDSL(relNode));
//
//    }
//
//    @Test
//    public void testLeftJoin() throws IOException {
//        Schema schema = leftJoin(eq(id("id"), id("id2")), fromTable("db1", "travelrecord"), projectNamed(fromTable("db1", "travelrecord2"), "id2", "user_id2"));
//        Assert.assertEquals("JoinSchema(type=LEFT_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id2, user_id2], fieldSchemaList=[])], condition=eq(Identifier(value=id),Identifier(value=id2)))", schema.toString());
//
//        RelNode relNode = toRelNode(schema);
//        Assert.assertEquals("LogicalJoin(condition=[=($2, $0)], joinType=[left])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalProject(id2=[$0], user_id2=[$1])\n" +
//                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
//        dump(relNode);
//    }
//
//    @Test
//    public void testRightJoin() throws IOException {
//        Schema schema = rightJoin(eq(id("id"), id("id0")), fromTable("db1", "travelrecord"), projectNamed(fromTable("db1", "travelrecord2"), "id0", "user_id0"));
//        Assert.assertEquals("JoinSchema(type=RIGHT_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(Identifier(value=id),Identifier(value=id0)))", schema.toString());
//
//        RelNode relNode = toRelNode(schema);
//        Assert.assertEquals("LogicalJoin(condition=[=($2, $0)], joinType=[right])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalProject(id0=[$0], user_id0=[$1])\n" +
//                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
//        dump(relNode);
//    }
//
//    @Test
//    public void testFullJoin() throws IOException {
//        Schema schema = fullJoin(eq(id("id0"), id("id")), fromTable("db1", "travelrecord"), projectNamed(fromTable("db1", "travelrecord2"), "id0", "user_id0"));
//        Assert.assertEquals("JoinSchema(type=FULL_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(Identifier(value=id0),Identifier(value=id)))", schema.toString());
//
//
//        RelNode relNode = toRelNode(schema);
//        Assert.assertEquals("LogicalJoin(condition=[=($0, $2)], joinType=[full])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalProject(id0=[$0], user_id0=[$1])\n" +
//                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
//        dump(relNode);
//    }
//
//    @Test
//    public void testSemiJoin() throws IOException {
//        Schema schema = semiJoin(eq(id("id0"), id("id")), fromTable("db1", "travelrecord"), projectNamed(fromTable("db1", "travelrecord2"), "id0", "user_id0"));
//        Assert.assertEquals("JoinSchema(type=SEMI_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(Identifier(value=id0),Identifier(value=id)))", schema.toString());
//
//
//        RelNode relNode = toRelNode(schema);
//        Assert.assertEquals("LogicalJoin(condition=[=($0, $2)], joinType=[semi])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalProject(id0=[$0], user_id0=[$1])\n" +
//                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
//        dump(relNode);
//    }
//
//    @Test
//    public void testAntiJoin() throws IOException {
//        Schema schema = antiJoin(eq(id("id0"), id("id")),
//                fromTable("db1", "travelrecord"),
//                projectNamed(fromTable("db1", "travelrecord2"), "id0", "user_id0"));
//        Assert.assertEquals("JoinSchema(type=ANTI_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(Identifier(value=id0),Identifier(value=id)))", schema.toString());
//
//        RelNode relNode = toRelNode(schema);
//        Assert.assertEquals("LogicalJoin(condition=[=($0, $2)], joinType=[anti])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalProject(id0=[$0], user_id0=[$1])\n" +
//                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
//        dump(relNode);
//    }
//
//    @Test
//    public void testCorrelateInnerJoin() throws IOException {
//        Schema correlate = correlateInnerJoin(id("t"), keys(id("id")),
//                fromTable("db1", "travelrecord"),
//                filter(projectNamed(fromTable("db1", "travelrecord2"), "id0", "user_id0"),
//                        eq(ref("t", "id"), id("id0"))));
//        RelNode relNode = toRelNode(correlate);
//        Assert.assertEquals("LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalFilter(condition=[=($cor0.id, $0)])\n" +
//                "    LogicalProject(id0=[$0], user_id0=[$1])\n" +
//                "      LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
//        dump(relNode);
//        Assert.assertEquals("correlateInnerJoin(`$cor0`,keys(`id`),fromTable(`db1`,`travelrecord`),filter(map(fromTable(`db1`,`travelrecord2`),as(`id`,`id0`),as(`user_id`,`user_id0`)),eq(ref(`$cor0`,`id`),`id0`)))", toDSL(relNode));
//    }
//
//    @Test
//    public void testCorrelateLeftJoin() throws IOException {
//        Schema correlate = correlateLeftJoin(id("t"), keys(id("id")),
//                fromTable("db1", "travelrecord"),
//                filter(projectNamed(fromTable("db1", "travelrecord2"), "id0", "user_id0"),
//                        eq(ref("t", "id"), id("id0"))));
//        RelNode relNode = toRelNode(correlate);
//        Assert.assertEquals("LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalFilter(condition=[=($cor0.id, $0)])\n" +
//                "    LogicalProject(id0=[$0], user_id0=[$1])\n" +
//                "      LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
//        dump(relNode);
//        Assert.assertEquals("correlateLeftJoin(`$cor0`,keys(`id`),fromTable(`db1`,`travelrecord`),filter(map(fromTable(`db1`,`travelrecord2`),as(`id`,`id0`),as(`user_id`,`user_id0`)),eq(ref(`$cor0`,`id`),`id0`)))", toDSL(relNode));
//    }
//
////    @Test
////    public void testCorrelateLeftJoCorrelateSchemain() throws IOException {
////        Schema schema = correlateLeftJoin(eq(ref("t","id"), id("id")), correlate(fromTable("db1", "travelrecord"),"t"), projectNamed(fromTable("db1", "travelrecord2"),"id0","user_id0"));
////        Assert.assertEquals("JoinSchema(type=CORRELATE_LEFT_JOIN, schemas=[CorrelateSchema(fromTable=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), refName=t), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(ref(Identifier(value=t),Identifier(value=id)),Identifier(value=id)))", schema.toString());
////
////        RelNode relNode = toRelNode(schema);
////        Assert.assertEquals("LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{}])\n" +
////                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
////                "  LogicalFilter(condition=[=($cor0.id, $0)])\n" +
////                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
////        dump(relNode);
////
////        Assert.assertEquals("projectNamed(join(correlateLeftJoin,correlate(as(fromTable(`db1`,`travelrecord`),`$cor0`)),as(filter(fromTable(`db1`,`travelrecord2`),eq(dot(`$cor0`,`id`),`id`)),`t1`)),`id`,`user_id`,`id0`,`user_id0`)", toDSL(relNode));
////        relNode = toRelNode(schema);
////        Assert.assertEquals("LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{}])\n" +
////                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
////                "  LogicalFilter(condition=[=($cor0.id, $0)])\n" +
////                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
////
////        schema = map(correlateInnerJoin(correlate(fromTable("db1", "travelrecord"),"t"), fromTable("db1", "travelrecord2")), ref("t", "id"));
////        relNode = toRelNode(schema);
////        Assert.assertEquals("projectNamed(join(correlateLeftJoin,correlate(as(fromTable(`db1`,`travelrecord`),`$cor0`)),as(filter(fromTable(`db1`,`travelrecord2`),eq(dot(`$cor0`,`id`),`id`)),`t1`)),`id`,`user_id`,`id0`,`user_id0`)", toDSL(relNode));
////
////
////        dump(relNode);
////
////    }
//
//    @Test
//    public void test() {
//        Schema select;
//        select = valuesSchema(fields(fieldType("1", "int")), values((1)));
//
//        RelNode relNode = toRelNode(select);
////
////        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER 1)], tuples=[[]])\n", toString(relNode));
//
//
//
//        Schema schema = convertRelNode(relNode);
//        String sb = toString(schema);
//        System.out.println(sb);
//        System.out.println(schema.toString());
//    }
//
//    private String toString(Schema schema) {
//        ExplainVisitor explainVisitor = new ExplainVisitor();
//        schema.accept(explainVisitor);
//        return explainVisitor.getString();
//    }
//
//    private String toDSL(RelNode relNode) {
//        return toString(convertRelNode(relNode));
//    }
//
//    private String toDSL(RexNode rexNode) {
//        return convertRexNode(rexNode).toString();
//    }
//
//    private List<Expr> convertRexNode(List<RexNode> rexNodes) {
//        return rexNodes.stream().map(i -> convertRexNode(i)).collect(Collectors.toList());
//    }
//
//    @Test
//    public void selectFromGroupByKeyAvg() throws IOException {
//        RelNode relNode;
//        Schema schema = group(fromTable("db1", "travelrecord"), keys(regular(id("id"))), aggregating(avg("id")));
//        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='avg', alias='avg(id)', operands=[Identifier(value=id)]])", schema.toString());
//
//        String text = "group(fromTable(db1,travelrecord),keys(regular(id)), aggregating(avg(id)))";
//        Assert.assertEquals("group(fromTable(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(avg(id(\"id\"))))", getS(parse2SyntaxAst(text)));
//
//        Assert.assertEquals("LogicalAggregate(group=[{0}], avg(id)=[AVG($0)])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
//
//
//        String dsl = toDSL(relNode = toRelNode(schema));
//        Assert.assertEquals("group(fromTable(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`avg`,`id`),`avg(id)`)))", dsl);
//    }
//

//
//    //////////////////////////////////////////
//    void visitFieldSchema(String id, String type) {
//
//    }
//}
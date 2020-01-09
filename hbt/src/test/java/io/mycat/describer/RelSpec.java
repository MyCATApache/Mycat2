package io.mycat.describer;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import io.mycat.DesRelNodeHandler;
import io.mycat.rsqlBuilder.DesBuilder;
import io.mycat.hbt.BaseQuery;
import io.mycat.hbt.Op;
import io.mycat.hbt.QueryOp;
import io.mycat.hbt.ast.AggregateCall;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.query.FieldType;
import io.mycat.hbt.ast.query.SetOpSchema;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.NlsString;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.mycat.DesRelNodeHandler.dump;
import static io.mycat.DesRelNodeHandler.parse2SyntaxAst;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.apache.calcite.sql.SqlExplainLevel.DIGEST_ATTRIBUTES;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;

public class RelSpec extends BaseQuery {

    private List<String> fieldNames;
    private FrameworkConfig config;



    @Before
    public void setUp() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema = rootSchema.add("db1", new ReflectiveSchema(new Db1()));
        this.config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema).build();
    }

    public RelNode toRelNode(Schema node) {
        return new QueryOp(DesBuilder.create(config)).complie(node);
    }

    public RexNode toRexNode(Expr node) {
        return new QueryOp(DesBuilder.create(config)).toRex(node);
    }

    private ParseNode getParseNode(String text) {
        Describer describer = new Describer(text);
        return describer.expression();
    }

    @Test
    public void l() throws IOException {


    }

    @Test
    public void selectWithoutFrom() throws IOException {
        Schema select;
        RelNode relNode;
        select = valuesSchema(fields(fieldType("1", "int")), values());
        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`int`)),values())", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "int")), values(1, 2, 3, 4, 5));
        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER 1)], tuples=[[{ 1 }, { 2 }, { 3 }, { 4 }, { 5 }]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`int`)),values(literal(1),literal(2),literal(3),literal(4),literal(5)))", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "int"), fieldType("2", "int")), values(1, 2, 3, 4));
        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER 1, INTEGER 2)], tuples=[[{ 1, 2 }, { 3, 4 }]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`int`),fieldType(`2`,`int`)),values(literal(1),literal(2),literal(3),literal(4)))", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "varchar"), fieldType("2", "varchar")), values("1", "2", "3", "4"));
        Assert.assertEquals("LogicalValues(type=[RecordType(VARCHAR 1, VARCHAR 2)], tuples=[[{ '1', '2' }, { '3', '4' }]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`varchar`),fieldType(`2`,`varchar`)),values(literal('1'),literal('2'),literal('3'),literal('4')))", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "float")), values());
        Assert.assertEquals("LogicalValues(type=[RecordType(FLOAT 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`float`)),values())", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "long")), values());
        Assert.assertEquals("LogicalValues(type=[RecordType(BIGINT 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`long`)),values())", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "varchar")), values());
        Assert.assertEquals("LogicalValues(type=[RecordType(VARCHAR 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`varchar`)),values())", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "varbinary")), values());
        Assert.assertEquals("LogicalValues(type=[RecordType(VARBINARY 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`varbinary`)),values())", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "varbinary")), values(new byte[]{'a'}));
        Assert.assertEquals("LogicalValues(type=[RecordType(VARBINARY 1)], tuples=[[{ X'61' }]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`varbinary`)),values(literal(X'61')))", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "date")), values());
        Assert.assertEquals("LogicalValues(type=[RecordType(DATE 1)], tuples=[[]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`date`)),values())", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "date")), values(date("2019-11-17")));
        Assert.assertEquals("LogicalValues(type=[RecordType(DATE 1)], tuples=[[{ 2019-11-17 }]])\n", toString(relNode = toRelNode(select)));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`date`)),values(dateLiteral(2019-11-17)))", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "time")), values(time("00:09:00")));
        Assert.assertTrue(toString(relNode = toRelNode(select)).contains("TIME"));
        LocalDateTime now = LocalDateTime.now();
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`time`)),values(timeLiteral(00:09)))", toDSL(relNode));

        select = valuesSchema(fields(fieldType("1", "timestamp")), values(timeStamp(now.toString())));
        Assert.assertEquals("valuesSchema(fields(fieldType(`1`,`time`)),values(timeLiteral(00:09)))", toDSL(relNode));

        Assert.assertTrue(toString(toRelNode(select)).contains("TIMESTAMP"));


    }

    static String getFieldName(List<String> fieldNames, int index) {
        if (fieldNames != null) {
            return fieldNames.get(index);
        } else {
            return "$" + index;
        }
    }

    String getFieldName(int index) {
        return getFieldName(this.fieldNames, index);
    }


    private String toString(RelNode relNode) {
        return RelOptUtil.toString(relNode, DIGEST_ATTRIBUTES).replaceAll("\r", "");
    }

    private String toString(RexNode relNode) {
        return relNode.toString();
    }


    @Test
    public void selectAllWithoutFrom() throws IOException {
        RelNode relNode1;

        Schema select = all(valuesSchema(fields(fieldType("1", "int")), values()));
        Assert.assertEquals("ValuesSchema(values=[], fieldNames=[FieldType(id=1, type=int)])", select.toString());

        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER 1)], tuples=[[]])\n", toString(toRelNode(select)));
    }

    @Test
    public void selectAllWithoutFrom2() throws IOException {
        String text = "all(valuesSchema(fields(fieldType(id,int)),values()))";
        ParseNode expression = getParseNode(text);
        Assert.assertEquals(text, expression.toString());
        String s = getS(expression);
        Assert.assertEquals("all(valuesSchema(fields(fieldType(id(\"id\"),id(\"int\"))),values()))", s);


        Schema select = all(valuesSchema(fields(fieldType("id", "int")), values()));
        Assert.assertEquals("ValuesSchema(values=[], fieldNames=[FieldType(id=id, type=int)])", select.toString());

        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER id)], tuples=[[]])\n", toString(toRelNode(select)));
    }

    @Test
    public void selectWithoutFrom2() throws IOException {
        Schema anInt = map(valuesSchema(fields(fieldType("1", "int")), values()), eq(id("1"), literal(1)));
        RelNode relNode1 = toRelNode(anInt);
        String dsl = toDSL(relNode1);
        Assert.assertEquals("map(valuesSchema(fields(fieldType(`1`,`int`)),values()),as(eq(`1`,literal(1)),`$f0`))", dsl);
    }

    @Test
    public void selectDistinctWithoutFrom2() throws IOException {
        String text = "distinct(valuesSchema(fields(fieldType(id,int)),values()))";
        ParseNode expression = getParseNode(text);
        Assert.assertEquals(text, expression.toString());
        String s = getS(expression);
        Assert.assertEquals("distinct(valuesSchema(fields(fieldType(id(\"id\"),id(\"int\"))),values()))", s);

        Schema select = distinct(valuesSchema(fields(fieldType("id", "int")), values()));
        Assert.assertEquals("DistinctSchema(schema=ValuesSchema(values=[], fieldNames=[FieldType(id=id, type=int)]))", select.toString());
    }

    @Test
    public void selectDistinctWithoutFrom() throws IOException {
        Schema select = distinct(valuesSchema(fields(fieldType("1", "int")), values(2, 2)));
        Assert.assertEquals("DistinctSchema(schema=ValuesSchema(values=[Literal(value=2), Literal(value=2)], fieldNames=[FieldType(id=1, type=int)]))", select.toString());
        RelNode relNode = toRelNode(select);

        Assert.assertEquals("LogicalAggregate(group=[{0}])\n" +
                "  LogicalValues(type=[RecordType(INTEGER 1)], tuples=[[{ 2 }, { 2 }]])\n", toString(relNode));
        Assert.assertEquals("(2)\n", dump(relNode));

        String dsl = toDSL(relNode);
        Assert.assertEquals("group(valuesSchema(fields(fieldType(`1`,`int`)),values(literal(2),literal(2))),keys(regular(`1`)),aggregating())", dsl);
    }

    @Test
    public void selectProjectItemWithoutFrom2() throws IOException {
        String text = "project(valuesSchema(fields(fieldType(id,int),fieldType(id2,int)),values()),id3,id4)";
        ParseNode expression = getParseNode(text);
        Assert.assertEquals(text, expression.toString());
        String s = getS(expression);
        Assert.assertEquals("project(valuesSchema(fields(fieldType(id(\"id\"),id(\"int\")),fieldType(id(\"id2\"),id(\"int\"))),values()),id(\"id3\"),id(\"id4\"))", s);

        Schema select = projectNamed(valuesSchema(fields(fieldType("id", "int"), fieldType("id2", "varchar")), values()), "id3", "id4");
        Assert.assertEquals("ProjectSchema(schema=ValuesSchema(values=[], fieldNames=[FieldType(id=id, type=int), FieldType(id=id2, type=varchar)]), columnNames=[id3, id4], fieldSchemaList=[FieldType(id=id, type=int), FieldType(id=id2, type=varchar)])", select.toString());
    }

    @Test
    public void from() throws IOException {
        String text = "from(db1,travelrecord)";
        RelNode relNode;
        ParseNode expression = getParseNode(text);
        Assert.assertEquals(text, expression.toString());
        String s = getS(expression);
        Assert.assertEquals("from(id(\"db1\"),id(\"travelrecord\"))", s);

        Assert.assertEquals("FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])", from(id("db1"), id("travelrecord")).toString());

        Schema select = from("db1", "travelrecord");
        Assert.assertEquals("FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])", select.toString());

        Assert.assertEquals("LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(select)));

        String dsl = toDSL(relNode);
        Assert.assertEquals("from(`db1`,`travelrecord`)", dsl);
    }

    @Test
    public void selectProjectItemWithoutFrom() throws IOException {
        RelNode relNode;
        Schema select = projectNamed(valuesSchema(fields(fieldType("1", "int"), fieldType("2", "varchar")), values()), "2", "1");
        Assert.assertEquals("ProjectSchema(schema=ValuesSchema(values=[], fieldNames=[FieldType(id=1, type=int), FieldType(id=2, type=varchar)]), columnNames=[2, 1], fieldSchemaList=[FieldType(id=1, type=int), FieldType(id=2, type=varchar)])", select.toString());

        Assert.assertEquals("LogicalProject(2=[$0], 1=[$1])\n" +
                "  LogicalValues(type=[RecordType(INTEGER 1, VARCHAR 2)], tuples=[[]])\n", toString(relNode = toRelNode(select)));

        String dsl = toDSL(relNode);
        Assert.assertEquals("map(valuesSchema(fields(fieldType(`1`,`int`),fieldType(`2`,`varchar`)),values()),as(`1`,`2`),as(`2`,`1`))", dsl);
    }

    @Test
    public void selectUnionAll() throws IOException {
        RelNode relNode;
        Schema select = unionAll(from("db1", "travelrecord"), from("db1", "travelrecord"));
        Assert.assertEquals("SetOpSchema(op=UNION_ALL,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());

        String text = "from(db1,travelrecord) unionAll  from(\"db1\", \"travelrecord\")";
        String s = getS(parse2SyntaxAst(text));
        Assert.assertEquals("unionAll(from(id(\"db1\"),id(\"travelrecord\")),from(id(\"db1\"),id(\"travelrecord\")))", s);

        Assert.assertEquals("LogicalUnion(all=[true])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(select)));

        String dsl = toDSL(relNode = toRelNode(select));
        Assert.assertEquals("unionAll(from(`db1`,`travelrecord`),from(`db1`,`travelrecord`))", dsl);
    }

    @Test
    public void selectUnionDistinct() throws IOException {
        RelNode relNode;
        Schema select = unionDistinct(from("db1", "travelrecord"), from("db1", "travelrecord"));
        Assert.assertEquals("SetOpSchema(op=UNION_DISTINCT,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());

        String text = "from(db1,travelrecord) unionDistinct  from(\"db1\", \"travelrecord\")";
        Assert.assertEquals("unionDistinct(from(id(\"db1\"),id(\"travelrecord\")),from(id(\"db1\"),id(\"travelrecord\")))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalUnion(all=[false])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(select)));

        String dsl = toDSL(relNode = toRelNode(select));
        Assert.assertEquals("unionDistinct(from(`db1`,`travelrecord`),from(`db1`,`travelrecord`))", dsl);


    }

    private String getS(ParseNode parseNode) {
        return DesRelNodeHandler.syntaxAstToFlatSyntaxAstText(parseNode);
    }

    @Test
    public void selectProjectFrom() throws IOException {
        String text = "from(db1,travelrecord).project(id)";
        RelNode relNode;
        String s = getS(parse2SyntaxAst(text));
        Assert.assertEquals("project(from(id(\"db1\"),id(\"travelrecord\")),id(\"id\"))", s);

        Schema select = projectNamed(from("db1", "travelrecord"), "1", "2");
        Assert.assertEquals("ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), columnNames=[1, 2], fieldSchemaList=[])", select.toString());

        String dsl = toDSL(relNode = toRelNode(select));
        Assert.assertEquals("map(from(`db1`,`travelrecord`),as(`id`,`1`),as(`user_id`,`2`))", dsl);

        select = projectNamed(from("db1", "travelrecord"));
        Assert.assertEquals("ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), columnNames=[], fieldSchemaList=[])", select.toString());

        dsl = toDSL(relNode = toRelNode(select));
        Assert.assertEquals("from(`db1`,`travelrecord`)", dsl);
    }

    @Test
    public void selectExceptDistinct() throws IOException {
        RelNode relNode;
        Schema select = exceptDistinct(from("db1", "travelrecord"), from("db1", "travelrecord"));
        Assert.assertEquals("SetOpSchema(op=EXCEPT_DISTINCT,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());

        String text = "from(db1,travelrecord) exceptDistinct  from(db1,travelrecord)";
        Assert.assertEquals("exceptDistinct(from(id(\"db1\"),id(\"travelrecord\")),from(id(\"db1\"),id(\"travelrecord\")))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalMinus(all=[false])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(select)));

        String dsl = toDSL(relNode = toRelNode(select));
        Assert.assertEquals("exceptDistinct(from(`db1`,`travelrecord`),from(`db1`,`travelrecord`))", dsl);
    }

    @Test
    public void selectExceptAll() throws IOException {
        RelNode relNode;
        Schema select = exceptAll(from("db1", "travelrecord"), from("db1", "travelrecord"));
        Assert.assertEquals("SetOpSchema(op=EXCEPT_ALL,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());

        String text = "from(db1,travelrecord) exceptAll  from(\"db1\", \"travelrecord\")";
        Assert.assertEquals("exceptAll(from(id(\"db1\"),id(\"travelrecord\")),from(id(\"db1\"),id(\"travelrecord\")))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalMinus(all=[true])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(select)));

        String dsl = toDSL(relNode = toRelNode(select));
        Assert.assertEquals("exceptAll(from(`db1`,`travelrecord`),from(`db1`,`travelrecord`))", dsl);
    }

    @Test
    public void selectFromOrder() throws IOException {
        RelNode relNode;
        Schema schema = orderBy(from("db1", "travelrecord"), order("id", "ASC"), order("user_id", "DESC"));
        Assert.assertEquals("OrderSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), orders=[OrderItem(columnName=Identifier(value=id), direction=ASC), OrderItem(columnName=Identifier(value=user_id), direction=DESC)])", schema.toString());

        String text = "orderBy(from(db1,travelrecord),order(id,ASC), order(user_id,DESC))";
        Assert.assertEquals("orderBy(from(id(\"db1\"),id(\"travelrecord\")),order(id(\"id\"),id(\"asc\")),order(id(\"user_id\"),id(\"desc\")))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));

        String dsl = toDSL(relNode = toRelNode(schema));
        Assert.assertEquals("orderBy(from(`db1`,`travelrecord`),order(`id`,`ASC`),order(`user_id`,`DESC`))", dsl);
    }

    @Test
    public void selectFromLimit() throws IOException {
        RelNode relNode;
        Schema schema = limit(from("db1", "travelrecord"), 1, 1000);
        Assert.assertEquals("LimitSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), offset=Literal(value=1), limit=Literal(value=1000))", schema.toString());

        String text = "limit(from(db1,travelrecord),order(id,ASC), order(user_id,DESC),1,1000)";
        Assert.assertEquals("limit(from(id(\"db1\"),id(\"travelrecord\")),order(id(\"id\"),id(\"asc\")),order(id(\"user_id\"),id(\"desc\")),literal(1),literal(1000))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalSort(offset=[1], fetch=[1000])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));

        String dsl = toDSL(relNode = toRelNode(schema));
        Assert.assertEquals("limit(from(`db1`,`travelrecord`),1,1000)", dsl);
    }

    @Test
    public void selectFromGroupByKey() throws IOException {
        RelNode relNode;
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalAggregate(group=[{0}])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));

        String dsl = toDSL(relNode = toRelNode(schema));
        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating())", dsl);
    }

    @Test
    public void selectFromGroupByKeyCount() throws IOException {
        RelNode relNode;
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(count("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='count', alias='count(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(count(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(count(id(\"id\"))))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));

        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`count`,`id`),`count(id)`)))", toDSL(relNode));
    }

    @Test
    public void selectFromGroupByKeyCountStar() throws IOException {
        RelNode relNode;
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(count()));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='count', alias='count()', operands=[]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(countStar()))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(countStar()))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalAggregate(group=[{0}], count()=[COUNT()])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));

        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`count`),`count()`)))", toDSL(relNode));
    }


    @Test
    public void selectFromGroupByKeyCountDistinct() throws IOException {
        RelNode relNode;
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(distinct(count("id"))));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='count', distinct=true, alias='count(id)', operands=[Identifier(value=id)]])", schema.toString());
        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(distinct(as(call(`count`,`id`),`count(id)`))))", toDSL(relNode = toRelNode(schema)));

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(count(id).distinct()))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(distinct(count(id(\"id\")))))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT(DISTINCT $0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));

        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(all(distinct(count("id"))))))));
        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`count`,`id`),`count(id)`)))", toDSL(relNode));


        Assert.assertEquals("LogicalAggregate(group=[{0}], asName=[COUNT($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(from("db1", "travelrecord"), keys(regular(id("id"))),
                aggregating((as(count("id"), "asName")))))));
        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`count`,`id`),`asName`)))", toDSL(relNode));

        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(from("db1", "travelrecord"), keys(regular(id("id"))),
                aggregating((approximate(count("id"))))))));
        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(approximate(as(call(`count`,`id`),`count(id)`))))", toDSL(relNode));

        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(from("db1", "travelrecord"), keys(regular(id("id"))),
                aggregating((exact(count("id"))))))));
        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`count`,`id`),`count(id)`)))", toDSL(relNode));


        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0) FILTER $2])\n" +
                "  LogicalProject(id=[$0], user_id=[$1], $f2=[=($0, 1)])\n" +
                "    LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(from("db1", "travelrecord"), keys(regular(id("id"))),
                aggregating((filter(count("id"), eq(id("id"), literal(1)))))))));
        Assert.assertEquals("group(map(from(`db1`,`travelrecord`),as(`id`,`id`),as(`user_id`,`user_id`),as(eq(`id`,literal(1)),`$f2`)),keys(regular(`id`)),aggregating(filter(as(call(`count`,`id`),`count(id)`),`$f2`)))", toDSL(relNode));

        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(from("db1", "travelrecord"), keys(regular(id("id"))),
                aggregating((ignoreNulls(count("id"))))))));
        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(ignoreNulls(as(call(`count`,`id`),`count(id)`))))", toDSL(relNode));

        Assert.assertEquals("LogicalAggregate(group=[{0}], count(id)=[COUNT($0) WITHIN GROUP ([0 DESC])])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(group(from("db1", "travelrecord"), keys(regular(id("id"))),
                aggregating((sort(count("id"), order("id", "DESC"))))))));
        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(sort(as(call(`count`,`id`),`count(id)`),order(id,desc))))", toDSL(relNode));

    }

    @Test
    public void selectFromGroupByKeyFirst() throws IOException {
        RelNode relNode;
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(first("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='first', alias='first(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(first(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(first(id(\"id\"))))", getS(parse2SyntaxAst(text)));


        Assert.assertEquals("LogicalAggregate(group=[{0}], first(id)=[FIRST_VALUE($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));
        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`first`,`id`),`first(id)`)))", toDSL(relNode));

    }

    @Test
    public void selectFromGroupByKeyLast() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(last("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='last', alias='last(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(last(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(last(id(\"id\"))))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalAggregate(group=[{0}], last(id)=[LAST_VALUE($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));


    }

    @Test
    public void selectFromGroupByKeyMax() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(max("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='max', alias='max(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(max(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(max(id(\"id\"))))", getS(parse2SyntaxAst(text)));

        String text2 = "from(db1,travelrecord).group(keys(regular(id)),aggregating(max(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(max(id(\"id\"))))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("LogicalAggregate(group=[{0}], max(id)=[MAX($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
    }

    @Test
    public void selectFromGroupByKeyMin() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(min("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='min', alias='min(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text2 = "from(db1,travelrecord).group(keys(regular(id)),aggregating(min(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(min(id(\"id\"))))", getS(parse2SyntaxAst(text2)));


        Assert.assertEquals("LogicalAggregate(group=[{0}], min(id)=[MIN($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
    }

    @Test
    public void selectUpperFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), upper("id"));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[upper(Identifier(value=id))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(upper(id))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),upper(id(\"id\")))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("LogicalProject($f0=[UPPER($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
    }

    @Test
    public void selectLowerFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), lower("id"));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[lower(Identifier(value=id))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(lower(id))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),lower(id(\"id\")))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("LogicalProject($f0=[LOWER($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
    }

    @Test
    public void selectMidFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), mid("id", 1));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[mid(Identifier(value=id),Literal(value=1))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(mid(id))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),mid(id(\"id\")))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void selectLenFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), len("id"));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[len(Identifier(value=id))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(len(id))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),len(id(\"id\")))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void selectRoundFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), round("id", 2));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[round(Identifier(value=id),Literal(value=2))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(round(id,2))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),round(id(\"id\"),literal(2)))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("LogicalProject($f0=[ROUND($0, 2)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));
    }

    @Test
    public void selectNowFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), now());
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[now()])", schema.toString());

        String text2 = "from(db1,travelrecord).map(now())";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),now())", getS(parse2SyntaxAst(text2)));


    }

    @Test
    public void selectFormatFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), format(now(), "YYYY-MM-DD"));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[format(now(),Literal(value=YYYY-MM-DD))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(format(now(),'YYYY-MM-DD'))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),format(now(),literal(\"YYYY-MM-DD\")))", getS(parse2SyntaxAst(text2)));

    }

    @Test
    public void filterIn() throws IOException {
        RelNode relNode;
        Schema schema = filter(from("db1", "travelrecord"), in("id", 1, 2));
        Assert.assertEquals("FilterSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), exprs=[or(eq(Identifier(value=id),Literal(value=1)),eq(Identifier(value=id),Literal(value=2)))])", schema.toString());

        String text2 = "from(db1,travelrecord).filter(in(id,1,2))";
        Assert.assertEquals("filter(from(id(\"db1\"),id(\"travelrecord\")),in(id(\"id\"),literal(1),literal(2)))", getS(parse2SyntaxAst(text2)));


        Assert.assertEquals("LogicalFilter(condition=[OR(=($0, 1), =($0, 2))])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));
        Assert.assertEquals("filter(from(`db1`,`travelrecord`),or(eq(`id`,literal(1)),eq(`id`,literal(2))))", toDSL(relNode));
    }


    @Test
    public void filterBetween() throws IOException {
        RelNode relNode;
        Schema schema = filter(from("db1", "travelrecord"), between("id", 1, 4));
        Assert.assertEquals("FilterSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), exprs=[and(gte(Identifier(value=id),Literal(value=1)),lte(Identifier(value=id),Literal(value=4)))])", schema.toString());


        String text2 = "from(db1,travelrecord).filter(between(id,1,4))";
        Assert.assertEquals("filter(from(id(\"db1\"),id(\"travelrecord\")),between(id(\"id\"),literal(1),literal(4)))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("LogicalFilter(condition=[AND(>=($0, 1), <=($0, 4))])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(schema)));
        Assert.assertEquals("filter(from(`db1`,`travelrecord`),and(gte(`id`,literal(1)),lte(`id`,literal(4))))", toDSL(relNode));
    }

    @Test
    public void testIsnull() throws IOException {

        Assert.assertEquals("isnull(Identifier(value=id))", isnull(id("id")).toString());

        String text2 = "isnull(id)";
        Assert.assertEquals("isnull(id(\"id\"))", getS(parse2SyntaxAst(text2)));

        Expr expr = isnull(literal(1));
        Assert.assertEquals("IS NULL(1)", toString(toRexNode(expr)));
    }

    @Test
    public void testIfnull() throws IOException {
        Assert.assertEquals("ifnull(Identifier(value=id),Literal(value=default))", ifnull("id", "default").toString());

        String text2 = "ifnull(id)";
        Assert.assertEquals("ifnull(id(\"id\"))", getS(parse2SyntaxAst(text2)));

    }

    @Test
    public void testNullif() throws IOException {
        Expr expr = nullif("id", "default");
        Assert.assertEquals("nullif(Identifier(value=id),Literal(value=default))", expr.toString());

        String text2 = "nullif(id)";
        Assert.assertEquals("nullif(id(\"id\"))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("NULLIF(null:NULL, null:NULL)", toString(toRexNode(nullif(literal(null), literal(null)))));
    }

    @Test
    public void testIsNotNull() throws IOException {
        Expr expr = isnotnull("id");
        Assert.assertEquals("isnotnull(Identifier(value=id))", expr.toString());

        String text2 = "isnotnull(id)";
        Assert.assertEquals("isnotnull(id(\"id\"))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("IS NOT NULL(null:NULL)", toString(toRexNode(isnotnull(literal(null)))));

    }

    @Test
    public void testInteger() throws IOException {
        Expr expr = literal(1);
        RexNode rexNode;
        Assert.assertEquals("Literal(value=1)", expr.toString());

        String text2 = "1";
        Assert.assertEquals("literal(1)", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("1", toString(rexNode = toRexNode(literal(Integer.valueOf(1)))));

        Expr expr1 = getExpr(rexNode);
        Assert.assertEquals("Literal(value=1)", expr1.toString());
    }

    @Test
    public void testLong() throws IOException {
        Expr expr = literal(1L);
        RexNode rexNode;

        Assert.assertEquals("Literal(value=1)", expr.toString());
        String text2 = "1";
        Assert.assertEquals("literal(1)", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("1", toString(rexNode = toRexNode(literal(Long.valueOf(1)))));

        Expr expr1 = getExpr(rexNode);
        Assert.assertEquals("Literal(value=1)", expr1.toString());
    }

    @Test
    public void testFloat() throws IOException {
        Expr expr = literal(Float.MAX_VALUE);
        RexNode rexNode;

        Assert.assertEquals("Literal(value=3.4028234663852886E+38)", expr.toString());
        String text2 = String.valueOf(Float.MAX_VALUE);
        Assert.assertEquals("literal(3.4028235E+38)", getS(parse2SyntaxAst(text2)));
        Assert.assertEquals("1.0:DECIMAL(2, 1)", toString(rexNode = toRexNode(literal(Float.valueOf(1)))));

        Expr expr1 = getExpr(rexNode);
        Assert.assertEquals("Literal(value=1.0)", expr1.toString());
    }

    @Test
    public void testId() throws IOException {
        Expr expr = id("id");
        Assert.assertEquals("Identifier(value=id)", expr.toString());

        String text2 = "id";
        Assert.assertEquals("id(\"id\")", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testString() throws IOException {
        Expr expr = literal("str");
        Assert.assertEquals("Literal(value=str)", expr.toString());

        String text2 = "'str'";
        Assert.assertEquals("literal(\"str\")", getS(parse2SyntaxAst(text2)));

        RexNode rexNode = toRexNode(literal("str"));
        Assert.assertEquals("'str'", toString(rexNode));

        Expr expr1 = getExpr(rexNode);
        Assert.assertEquals("Literal(value=str)", expr1.toString());
    }

    @Test
    public void testTime() throws IOException {
        Expr expr = literal(time("00:09:00"));
        Assert.assertEquals("Literal(value=00:09)", expr.toString());

        String text2 = "time('00:09:00')";
        Assert.assertEquals("time(literal(\"00:09:00\"))", getS(parse2SyntaxAst(text2)));

        RexNode rexNode = toRexNode(timeLiteral("00:09:00"));
        Assert.assertEquals("00:09:00", toString(rexNode));

        Expr expr1 = getExpr(rexNode);
        Assert.assertEquals("Literal(value=00:09)", expr1.toString());
    }

    @Test
    public void testDate() throws IOException {
        Expr expr = literal(date("2019-11-17"));
        Assert.assertEquals("Literal(value=2019-11-17)", expr.toString());

        String text2 = "date('2019-11-17')";
        Assert.assertEquals("date(literal(\"2019-11-17\"))", getS(parse2SyntaxAst(text2)));
        RexNode rexNode = toRexNode(dateLiteral("2019-11-17"));
        Assert.assertEquals("2019-11-17", toString(rexNode));

        Expr expr1 = getExpr(rexNode);
        Assert.assertEquals("Literal(value=2019-11-17)", expr1.toString());
    }

    @Test
    public void testTimeStamp() throws IOException {


        LocalDateTime now = LocalDateTime.parse("2019-11-22T12:12:03");
        String text = now.toString();
        Expr expr = literal(timeStamp(text));
        Assert.assertEquals("Literal(value=" + text +
                ")", expr.toString());

        String text2 = "timeStamp('" +
                text +
                "')";
        Assert.assertEquals("timeStamp(literal(\"" +
                text +
                "\"))", getS(parse2SyntaxAst(text2)));
        RexNode rexNode = toRexNode(timeStampLiteral(text));
        Assert.assertEquals("2019-11-22 12:12:03", toString(rexNode));

        Expr expr1 = getExpr(rexNode);
        Assert.assertEquals("Literal(value=" +
                text +
                ")", expr1.toString());
    }

    @Test
    public void testMinus() throws IOException {
        Expr expr = minus(id("id"), literal(1));
        RexNode rexNode;
        Assert.assertEquals("minus(Identifier(value=id),Literal(value=1))", expr.toString());

        String text2 = "id-1";
        Assert.assertEquals("minus(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("-(1, 1)", toString(rexNode = toRexNode(minus(literal(1), literal(1)))));
        String dsl = toDSL(rexNode);
    }

    @Test
    public void testEqual() throws IOException {
        Expr expr = eq(id("id"), literal(1));
        Assert.assertEquals("eq(Identifier(value=id),Literal(value=1))", expr.toString());

        String text2 = "id=1";
        Assert.assertEquals("eq(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("=(1, 1)", toString(toRexNode(eq(literal(1), literal(1)))));
    }

    @Test
    public void testAnd() throws IOException {
        Expr expr = and(literal(1), literal(1));
        Assert.assertEquals("and(Literal(value=1),Literal(value=1))", expr.toString());

        String text2 = "1 and 1";
        Assert.assertEquals("and(literal(1),literal(1))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("AND(1, 1)", toString(toRexNode(expr)));
    }

    @Test
    public void testor() throws IOException {
        Expr expr = or(literal(1), literal(1));
        Assert.assertEquals("or(Literal(value=1),Literal(value=1))", expr.toString());

        String text2 = "1 or 1";
        Assert.assertEquals("or(literal(1),literal(1))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("OR(1, 1)", toString(toRexNode(expr)));
    }

    @Test
    public void testEqualOr() throws IOException {
        String text2 = "1 = 2 or 1 = 1";
        Assert.assertEquals("or(eq(literal(1),literal(2)),eq(literal(1),literal(1)))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("OR(=(1, 2), =(1, 1))", toString(toRexNode(or(eq(literal(1), literal(2)), eq(literal(1), literal(1))))));
    }

    @Test
    public void testNot() throws IOException {
        Expr expr = not(literal(1));
        Assert.assertEquals("not(Literal(value=1))", expr.toString());

        String text2 = "not(1 = 1)";
        Assert.assertEquals("not(eq(literal(1),literal(1)))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("NOT(=(1, 1))", toString(toRexNode(not(eq(literal(1), literal(1))))));

    }

    @Test
    public void testNotEqual() throws IOException {
        Expr expr = ne(id("id"), literal(1));
        Assert.assertEquals("ne(Identifier(value=id),Literal(value=1))", expr.toString());

        String text2 = "id <> 1";
        Assert.assertEquals("ne(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("NOT(<>(1, 1))", toString(toRexNode(not(ne(literal(1), literal(1))))));
    }

    @Test
    public void testGreaterThan() throws IOException {
        Expr expr = gt(id("id"), literal(1));
        Assert.assertEquals("gt(Identifier(value=id),Literal(value=1))", expr.toString());

        String text2 = "id > 1";
        Assert.assertEquals("gt(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals(">(1, 1)", toString(toRexNode((gt(literal(1), literal(1))))));
    }

    @Test
    public void testGreaterThanEqual() throws IOException {
        Expr expr = gte(id("id"), literal(true));
        Assert.assertEquals("gte(Identifier(value=id),Literal(value=true))", expr.toString());

        String text2 = "id >= 1";
        Assert.assertEquals("gte(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals(">=(1, 1)", toString(toRexNode((gte(literal(1), literal(1))))));
    }

    @Test
    public void testLessThan() throws IOException {
        Expr expr = lt(id("id"), literal(true));
        Assert.assertEquals("lt(Identifier(value=id),Literal(value=true))", expr.toString());

        String text2 = "id < 1";
        Assert.assertEquals("lt(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("<(1, 1)", toString(toRexNode((lt(literal(1), literal(1))))));
    }

    @Test
    public void testLessThanEqual() throws IOException {
        Expr expr = lte(id("id"), literal(true));
        Assert.assertEquals("lte(Identifier(value=id),Literal(value=true))", expr.toString());

        String text2 = "id <= 1";
        Assert.assertEquals("lte(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("<=(1, 1)", toString(toRexNode((lte(literal(1), literal(1))))));
    }

    @Test
    public void testAsColumnName() throws IOException {
        RelNode relNode;
        Expr expr = as(literal(1), id("column"));
        Assert.assertEquals("asColumnName(Literal(value=1),Identifier(value=column))", expr.toString());

        String text2 = "1 as column";
        Assert.assertEquals("as(literal(1),id(\"column\"))", getS(parse2SyntaxAst(text2)));

        Schema map = map(from("db1", "travelrecord"), as(id("user_id"), id("id")), as(literal(1), id("column")));
        Assert.assertEquals("LogicalProject(id=[$1], column=[1])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(relNode = toRelNode(map)));

        Assert.assertEquals("map(from(`db1`,`travelrecord`),as(`user_id`,`id`),as(literal(1),`column`))", toDSL(relNode));
    }

    @Test
    public void testCast() throws IOException {
        RexNode rexNode;
        Expr expr = cast(literal(1), id("float"));
        Assert.assertEquals("cast(Literal(value=1),Identifier(value=float))", expr.toString());

        String text2 = "cast(1,float)";
        Assert.assertEquals("cast(literal(1),id(\"float\"))", getS(parse2SyntaxAst(text2)));

        Assert.assertEquals("1:FLOAT", toString(rexNode = toRexNode(expr)));
        Assert.assertEquals("Literal(value=1)", toDSL(rexNode));

        Assert.assertEquals("map(from(`db1`,`travelrecord`),as(cast(`id`,`float`),`id`))", toDSL(toRelNode(map((from("db1", "travelrecord")), cast(id("id"), id("float"))))));
    }

    @Test
    public void testInnerJoin() throws IOException, SQLException {

        Schema schema = innerJoin(eq(id("id0"), id("id")),
                from("db1", "travelrecord"),
                projectNamed(from("db1", "travelrecord2"), "id0", "user_id0"));
        Assert.assertEquals("JoinSchema(type=INNER_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(Identifier(value=id0),Identifier(value=id)))", schema.toString());

        String text2 = "innerJoin(table.id = table2.id , from(db1,travelrecord),from(db1,travelrecord2))";
        Assert.assertEquals("innerJoin(eq(dot(id(\"table\"),id(\"id\")),dot(id(\"table2\"),id(\"id\"))),from(id(\"db1\"),id(\"travelrecord\")),from(id(\"db1\"),id(\"travelrecord2\")))",
                getS(parse2SyntaxAst(text2)));

        RelNode relNode = toRelNode(schema);
        Assert.assertEquals("LogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalProject(id0=[$0], user_id0=[$1])\n" +
                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
        dump(relNode);

        Assert.assertEquals("join(innerJoin,eq(`id`,`id0`),from(`db1`,`travelrecord`),map(from(`db1`,`travelrecord2`),as(`id`,`id0`),as(`user_id`,`user_id0`)))",
                toDSL(relNode));

    }

    @Test
    public void testLeftJoin() throws IOException {
        Schema schema = leftJoin(eq(id("id"), id("id2")), from("db1", "travelrecord"), projectNamed(from("db1", "travelrecord2"), "id2", "user_id2"));
        Assert.assertEquals("JoinSchema(type=LEFT_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id2, user_id2], fieldSchemaList=[])], condition=eq(Identifier(value=id),Identifier(value=id2)))", schema.toString());

        RelNode relNode = toRelNode(schema);
        Assert.assertEquals("LogicalJoin(condition=[=($2, $0)], joinType=[left])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalProject(id2=[$0], user_id2=[$1])\n" +
                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
        dump(relNode);
    }

    @Test
    public void testRightJoin() throws IOException {
        Schema schema = rightJoin(eq(id("id"), id("id0")), from("db1", "travelrecord"), projectNamed(from("db1", "travelrecord2"), "id0", "user_id0"));
        Assert.assertEquals("JoinSchema(type=RIGHT_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(Identifier(value=id),Identifier(value=id0)))", schema.toString());

        RelNode relNode = toRelNode(schema);
        Assert.assertEquals("LogicalJoin(condition=[=($2, $0)], joinType=[right])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalProject(id0=[$0], user_id0=[$1])\n" +
                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
        dump(relNode);
    }

    @Test
    public void testFullJoin() throws IOException {
        Schema schema = fullJoin(eq(id("id0"), id("id")), from("db1", "travelrecord"), projectNamed(from("db1", "travelrecord2"), "id0", "user_id0"));
        Assert.assertEquals("JoinSchema(type=FULL_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(Identifier(value=id0),Identifier(value=id)))", schema.toString());


        RelNode relNode = toRelNode(schema);
        Assert.assertEquals("LogicalJoin(condition=[=($0, $2)], joinType=[full])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalProject(id0=[$0], user_id0=[$1])\n" +
                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
        dump(relNode);
    }

    @Test
    public void testSemiJoin() throws IOException {
        Schema schema = semiJoin(eq(id("id0"), id("id")), from("db1", "travelrecord"), projectNamed(from("db1", "travelrecord2"), "id0", "user_id0"));
        Assert.assertEquals("JoinSchema(type=SEMI_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(Identifier(value=id0),Identifier(value=id)))", schema.toString());


        RelNode relNode = toRelNode(schema);
        Assert.assertEquals("LogicalJoin(condition=[=($0, $2)], joinType=[semi])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalProject(id0=[$0], user_id0=[$1])\n" +
                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
        dump(relNode);
    }

    @Test
    public void testAntiJoin() throws IOException {
        Schema schema = antiJoin(eq(id("id0"), id("id")),
                from("db1", "travelrecord"),
                projectNamed(from("db1", "travelrecord2"), "id0", "user_id0"));
        Assert.assertEquals("JoinSchema(type=ANTI_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(Identifier(value=id0),Identifier(value=id)))", schema.toString());

        RelNode relNode = toRelNode(schema);
        Assert.assertEquals("LogicalJoin(condition=[=($0, $2)], joinType=[anti])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalProject(id0=[$0], user_id0=[$1])\n" +
                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
        dump(relNode);
    }

    @Test
    public void testCorrelateInnerJoin() throws IOException {
        Schema correlate = correlateInnerJoin(id("t"), keys(id("id")),
                from("db1", "travelrecord"),
                filter(projectNamed(from("db1", "travelrecord2"), "id0", "user_id0"),
                        eq(ref("t", "id"), id("id0"))));
        RelNode relNode = toRelNode(correlate);
        Assert.assertEquals("LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalFilter(condition=[=($cor0.id, $0)])\n" +
                "    LogicalProject(id0=[$0], user_id0=[$1])\n" +
                "      LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
        dump(relNode);
        Assert.assertEquals("correlateInnerJoin(`$cor0`,keys(`id`),from(`db1`,`travelrecord`),filter(map(from(`db1`,`travelrecord2`),as(`id`,`id0`),as(`user_id`,`user_id0`)),eq(ref(`$cor0`,`id`),`id0`)))", toDSL(relNode));
    }

    @Test
    public void testCorrelateLeftJoin() throws IOException {
        Schema correlate = correlateLeftJoin(id("t"), keys(id("id")),
                from("db1", "travelrecord"),
                filter(projectNamed(from("db1", "travelrecord2"), "id0", "user_id0"),
                        eq(ref("t", "id"), id("id0"))));
        RelNode relNode = toRelNode(correlate);
        Assert.assertEquals("LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
                "  LogicalFilter(condition=[=($cor0.id, $0)])\n" +
                "    LogicalProject(id0=[$0], user_id0=[$1])\n" +
                "      LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
        dump(relNode);
        Assert.assertEquals("correlateLeftJoin(`$cor0`,keys(`id`),from(`db1`,`travelrecord`),filter(map(from(`db1`,`travelrecord2`),as(`id`,`id0`),as(`user_id`,`user_id0`)),eq(ref(`$cor0`,`id`),`id0`)))", toDSL(relNode));
    }

//    @Test
//    public void testCorrelateLeftJoCorrelateSchemain() throws IOException {
//        Schema schema = correlateLeftJoin(eq(ref("t","id"), id("id")), correlate(from("db1", "travelrecord"),"t"), projectNamed(from("db1", "travelrecord2"),"id0","user_id0"));
//        Assert.assertEquals("JoinSchema(type=CORRELATE_LEFT_JOIN, schemas=[CorrelateSchema(from=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), refName=t), ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord2)]), columnNames=[id0, user_id0], fieldSchemaList=[])], condition=eq(ref(Identifier(value=t),Identifier(value=id)),Identifier(value=id)))", schema.toString());
//
//        RelNode relNode = toRelNode(schema);
//        Assert.assertEquals("LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{}])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalFilter(condition=[=($cor0.id, $0)])\n" +
//                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
//        dump(relNode);
//
//        Assert.assertEquals("projectNamed(join(correlateLeftJoin,correlate(as(from(`db1`,`travelrecord`),`$cor0`)),as(filter(from(`db1`,`travelrecord2`),eq(dot(`$cor0`,`id`),`id`)),`t1`)),`id`,`user_id`,`id0`,`user_id0`)", toDSL(relNode));
//        relNode = toRelNode(schema);
//        Assert.assertEquals("LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{}])\n" +
//                "  LogicalTableScan(table=[[db1, travelrecord]])\n" +
//                "  LogicalFilter(condition=[=($cor0.id, $0)])\n" +
//                "    LogicalTableScan(table=[[db1, travelrecord2]])\n", toString(relNode));
//
//        schema = map(correlateInnerJoin(correlate(from("db1", "travelrecord"),"t"), from("db1", "travelrecord2")), ref("t", "id"));
//        relNode = toRelNode(schema);
//        Assert.assertEquals("projectNamed(join(correlateLeftJoin,correlate(as(from(`db1`,`travelrecord`),`$cor0`)),as(filter(from(`db1`,`travelrecord2`),eq(dot(`$cor0`,`id`),`id`)),`t1`)),`id`,`user_id`,`id0`,`user_id0`)", toDSL(relNode));
//
//
//        dump(relNode);
//
//    }

    @Test
    public void test() {
        Schema select;
        select = valuesSchema(fields(fieldType("1", "int")), values((1)));

        RelNode relNode = toRelNode(select);
//
//        Assert.assertEquals("LogicalValues(type=[RecordType(INTEGER 1)], tuples=[[]])\n", toString(relNode));



        Schema schema = getSchema(relNode);
        String sb = toString(schema);
        System.out.println(sb);
        System.out.println(schema.toString());
    }

    private String toString(Schema schema) {
        ExplainVisitor explainVisitor = new ExplainVisitor();
        schema.accept(explainVisitor);
        return explainVisitor.getSb();
    }

    private String toDSL(RelNode relNode) {
        return toString(getSchema(relNode));
    }

    private String toDSL(RexNode rexNode) {
        return getExpr(rexNode).toString();
    }

    private List<Expr> getExpr(List<RexNode> rexNodes) {
        return rexNodes.stream().map(i -> getExpr(i)).collect(Collectors.toList());
    }

    @Test
    public void selectFromGroupByKeyAvg() throws IOException {
        RelNode relNode;
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(avg("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='avg', alias='avg(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(avg(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(avg(id(\"id\"))))", getS(parse2SyntaxAst(text)));

        Assert.assertEquals("LogicalAggregate(group=[{0}], avg(id)=[AVG($0)])\n" +
                "  LogicalTableScan(table=[[db1, travelrecord]])\n", toString(toRelNode(schema)));


        String dsl = toDSL(relNode = toRelNode(schema));
        Assert.assertEquals("group(from(`db1`,`travelrecord`),keys(regular(`id`)),aggregating(as(call(`avg`,`id`),`avg(id)`)))", dsl);
    }

    private Expr getExpr(List<String> root, RexNode rexNode) {
        this.fieldNames = root;
        try {
            return getExpr(rexNode);
        } finally {
            this.fieldNames = null;
        }
    }

    private List<Expr> getExpr(List<String> fieldNames, List<RexNode> rexNode) {
        this.fieldNames = fieldNames;
        try {
            return getExpr(rexNode);
        } finally {
            this.fieldNames = null;
        }
    }

    private String op(SqlOperator kind) {
        return QueryOp.sqlOperatorMap.inverse().get(kind);
    }

    private List<Schema> getSchema(List<RelNode> relNodes) {
        return relNodes.stream().map(i -> getSchema(i)).collect(Collectors.toList());
    }

    private Expr getExpr(RexNode rexNode) {
        if (rexNode instanceof RexLiteral) {
            RexLiteral rexNode1 = (RexLiteral) rexNode;
            return literal(unWrapper(rexNode1));
        }
        if (rexNode instanceof RexInputRef) {
            RexInputRef expr = (RexInputRef) rexNode;
            return id(getFieldName(expr.getIndex()));
        }
        if (rexNode instanceof RexCall) {
            RexCall expr = (RexCall) rexNode;
            List<Expr> exprList = getExpr(expr.getOperands());
            if (expr.getOperator() == CAST) {
                ArrayList<Expr> args = new ArrayList<>(exprList.size() + 1);
                args.addAll(exprList);
                args.add(id(type(expr.getType().getSqlTypeName())));
                return funWithSimpleAlias(op(expr.op), args);
            } else {
                return funWithSimpleAlias(op(expr.op), exprList);
            }
        }
        if (rexNode instanceof RexFieldAccess) {
            RexFieldAccess rexNode1 = (RexFieldAccess) rexNode;
            Expr expr = getExpr(rexNode1.getReferenceExpr());
            return ref(expr, id(rexNode1.getField().getName()));
        }
        if (rexNode instanceof RexCorrelVariable) {
            RexCorrelVariable referenceExpr = (RexCorrelVariable) rexNode;
            return id(referenceExpr.getName());
        }
        return null;
    }

    private Schema getSchema(RelNode relNode) {
        List<RelNode> inputs = relNode.getInputs();
        String relTypeName = relNode.getRelTypeName();
        String correlVariable = relNode.getCorrelVariable();
        RelOptTable table = relNode.getTable();
        Set<CorrelationId> variablesSet = relNode.getVariablesSet();
        switch (relTypeName) {
            case "LogicalValues": {
                return logicValues(relNode);
            }
            case "LogicalProject": {
                return logicProject(relNode);
            }
            case "LogicalAggregate": {
                return logicalAggregate(relNode);
            }
            case "LogicalTableScan": {
                return logicalTableScan(relNode);
            }
            case "LogicalIntersect":
            case "LogicalMinus":
            case "LogicalUnion": {
                return logicalSetOp(relNode);
            }
            case "LogicalSort": {
                return logicalSort(relNode);
            }
            case "LogicalFilter": {
                return logicalFilter(relNode);
            }
            case "LogicalJoin": {
                return logicalJoin(relNode);
            }
            case "LogicalCorrelate": {
                return logicalCorrelate(relNode);
            }
        }
        throw new UnsupportedOperationException();
    }

    private Schema logicalCorrelate(RelNode relNode) {
        LogicalCorrelate relNode1 = (LogicalCorrelate) relNode;
        String correlVariable = relNode1.getCorrelVariable();
        Schema left = getSchema(relNode1.getLeft());
        Schema right = getSchema(relNode1.getRight());
        List<Integer> integers = relNode1.getRequiredColumns().asList();
        List<String> fieldNames = relNode1.getRowType().getFieldNames();
        List<Identifier> reqNames = integers.stream().map(i -> id(fieldNames.get(i))).collect(Collectors.toList());
        return correlate(joinType(relNode1.getJoinType(), true), id(correlVariable), reqNames, left, right);
    }

    private Schema logicalJoin(RelNode relNode) {
        LogicalJoin join = (LogicalJoin) relNode;
        JoinRelType joinType = join.getJoinType();
        RexNode condition = join.getCondition();
        List<String> fieldList = join.getRowType().getFieldNames();
        return join(joinType(joinType, false), getExpr(fieldList, condition), getSchema(join.getInputs()));
    }

    private Op joinType(JoinRelType joinType, boolean cor) {
        switch (joinType) {
            case INNER:
                return cor ? Op.CORRELATE_INNER_JOIN : Op.INNER_JOIN;
            case LEFT:
                return cor ? Op.CORRELATE_LEFT_JOIN : Op.LEFT_JOIN;
            case RIGHT:
                return Op.RIGHT_JOIN;
            case FULL:
                return Op.FULL_JOIN;
            case SEMI:
                return Op.SEMI_JOIN;
            case ANTI:
                return Op.ANTI_JOIN;
        }
        throw new UnsupportedOperationException();
    }

    private Schema logicalFilter(RelNode relNode) {
        LogicalFilter relNode1 = (LogicalFilter) relNode;
        return filter(getSchema(relNode1.getInput()), getExpr(relNode1.getInput().getRowType().getFieldNames(), relNode1.getCondition()));
    }

    private Schema logicalSort(RelNode relNode) {
        LogicalSort relNode1 = (LogicalSort) relNode;
        return getLimit((RexLiteral) relNode1.fetch, (RexLiteral) relNode1.offset, getOrderBy(getSchema(relNode1.getInput()), relNode1));
    }

    private Schema getOrderBy(Schema input, LogicalSort sort) {
        RelCollation collation = sort.getCollation();
        RelNode inputRel = sort.getInput();
        return orderBy(input, getOrderby(inputRel, collation));
    }

    @NotNull
    private List<OrderItem> getOrderby(RelNode inputRel, RelCollation collation) {
        return collation.getFieldCollations().stream().map(fieldCollation -> {
            RelFieldCollation.Direction direction = fieldCollation.getDirection();
            int fieldIndex = fieldCollation.getFieldIndex();
            return order(getFieldName(inputRel.getRowType().getFieldNames(), fieldIndex), op(direction));
        }).collect(Collectors.toList());
    }

    private String op(RelFieldCollation.Direction direction) {
        return direction == RelFieldCollation.Direction.DESCENDING ? "DESC" : "ASC";
    }

    private Schema getLimit(RexLiteral rexFetch, RexLiteral rexOffset, Schema input) {
        if (rexFetch != null || rexOffset != null) {
            Long offset = 0L;
            Long count = 0L;

            if (rexFetch != null) {
                count = rexFetch.getValueAs(Long.class);
            }

            if (rexOffset != null) {
                offset = rexOffset.getValueAs(Long.class);
            }
            return limit(input, offset, count);
        } else {
            return input;
        }
    }

    private Schema logicalSetOp(RelNode relNode) {
        SetOp logicalUnion = (SetOp) relNode;
        List<Schema> schema = getSchema(logicalUnion.getInputs());
        SqlKind kind = logicalUnion.kind;

        Schema first = schema.get(0);
        List<Schema> second = schema.subList(1, schema.size());
        switch (kind) {
            case UNION: {
                return logicalUnion.all ? unionAll(first, second) : unionDistinct(first, second);
            }
            case EXCEPT: {
                return logicalUnion.all ? exceptAll(first, second) : exceptDistinct(first, second);
            }
            case INTERSECT: {
                return logicalUnion.all ? intersectAll(first, second) : intersectDistinct(first, second);
            }
            default:
                throw new UnsupportedOperationException();
        }
    }

    public Schema intersectDistinct(Schema first, Schema... second) {
        return intersectDistinct(first, list(second));
    }

    public Schema intersectDistinct(Schema first, List<Schema> second) {
        return new SetOpSchema(Op.INTERSECT_DISTINCT, list(first, second));
    }

    public Schema intersectAll(Schema first, Schema... second) {
        return intersectAll(first, list(second));
    }

    public Schema intersectAll(Schema first, List<Schema> second) {
        return new SetOpSchema(Op.INTERSECT_ALL, list(first, second));
    }

    public Schema minusAll(Schema first, Schema... second) {
        return minusAll(first, list(second));
    }

    public Schema minusAll(Schema first, List<Schema> second) {
        return new SetOpSchema(Op.MINUS_ALL, list(first, second));
    }

    //    public Schema minusDistinct(Schema first, Schema... second) {
//        return minusDistinct(first,list(second));
//    }
//    public Schema minusDistinct(Schema first, List<Schema> second) {
//        return new SetOpSchema(Op.MINUS_DISTINCT,list(first,second));
//    }
    private Schema logicalTableScan(RelNode relNode) {
        LogicalTableScan tableScan = (LogicalTableScan) relNode;
        RelOptTable table = tableScan.getTable();
        RelDataType rowType = tableScan.getRowType();
        List<String> inputFieldNames = rowType.getFieldNames();
        List<String> outputFieldNames1 = table.getRowType().getFieldNames();

        List<String> qualifiedName = table.getQualifiedName();

        return from(qualifiedName.stream().map(i -> id(i)).collect(Collectors.toList()));
    }

    private Schema logicalAggregate(RelNode relNode) {
        LogicalAggregate relNode1 = (LogicalAggregate) relNode;
        Schema schema = getSchema(relNode1.getInput());
        Aggregate.Group groupType = relNode1.getGroupType();
        return group(schema, getGroupItems(relNode1), getAggCallList(relNode1.getInput(), relNode1.getAggCallList()));
    }

    private List<AggregateCall> getAggCallList(RelNode org, List<org.apache.calcite.rel.core.AggregateCall> aggCallList) {
        return aggCallList.stream().map(i -> getAggCallList(org, i)).collect(Collectors.toList());
    }

    private AggregateCall getAggCallList(RelNode inputRel, org.apache.calcite.rel.core.AggregateCall call) {
        List<String> fieldNames = inputRel.getRowType().getFieldNames();
        BiMap<SqlAggFunction, String> inverse = QueryOp.sqlAggFunctionMap.inverse();
        RelDataType type = call.getType();
        String alias = call.getName();
        String aggeName = inverse.get(call.getAggregation());
        List<Expr> argList = call.getArgList().stream().map(i -> id(fieldNames.get(i))).collect(Collectors.toList());
        boolean distinct = call.isDistinct();
        boolean approximate = call.isApproximate();
        boolean ignoreNulls = call.ignoreNulls();
        Expr filter = call.hasFilter() ? id(getFieldName(inputRel.getRowType().getFieldNames(), call.filterArg)) : null;
        List<OrderItem> orderby = getOrderby(inputRel, call.getCollation());
        return call(aggeName, alias, argList, distinct, approximate, ignoreNulls, filter, orderby);
    }

    private List<GroupItem> getGroupItems(LogicalAggregate groupSet) {
        List<GroupItem> list = new ArrayList<>();
        RelNode input = groupSet.getInput();
        for (Integer integer : groupSet.getGroupSet()) {
            list.add(regular(getFieldName(input.getRowType().getFieldNames(), integer)));
        }
        return list;
    }

    private Schema logicProject(RelNode relNode) {
        LogicalProject project = (LogicalProject) relNode;
        Schema schema = getSchema(project.getInput());
        List<Expr> expr = getExpr(project.getInput().getRowType().getFieldNames(), project.getChildExps());
        RelDataType outRowType = project.getRowType();
        List<String> outFieldNames = outRowType.getFieldNames();
        int size = outFieldNames.size();
        ArrayList<Expr> outExpr = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Expr expr1 = expr.get(i);
            outExpr.add(as(expr1, outFieldNames.get(i)));
        }
        return map(schema, outExpr);
    }

    private Schema logicValues(RelNode relNode) {
        LogicalValues logicalValues = (LogicalValues) relNode;
        return valuesSchema(getFieldSchema(relNode), getValues(logicalValues));
    }

    private List<Literal> getValues(LogicalValues relNode1) {
        ImmutableList<ImmutableList<RexLiteral>> tuples = relNode1.getTuples();
        if (tuples == null) {
            return Collections.emptyList();
        }
        return tuples.stream().flatMap(Collection::stream).map(rexLiteral -> literal(unWrapper(rexLiteral))).collect(Collectors.toCollection(ArrayList::new));
    }

    private Object unWrapper(RexLiteral rexLiteral) {
        if (rexLiteral.isNull()) {
            return null;
        }
        RelDataType type = rexLiteral.getType();
        SqlTypeName sqlTypeName = type.getSqlTypeName();
        switch (sqlTypeName) {
            case BOOLEAN:
            case SMALLINT:
            case TINYINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
                return rexLiteral.getValue();
            case DATE: {
                Integer valueAs = (Integer) rexLiteral.getValue4();
                return LocalDate.ofEpochDay(valueAs);
            }
            case TIME: {
                Integer value = (Integer) rexLiteral.getValue4();
                return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(value));
            }
            case TIME_WITH_LOCAL_TIME_ZONE:
                break;
            case TIMESTAMP:
                String s = rexLiteral.toString();
                DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(ISO_LOCAL_DATE)
                        .appendLiteral(' ')
                        .append(ISO_LOCAL_TIME)
                        .toFormatter();
                return LocalDateTime.parse(s, dateTimeFormatter);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                break;
            case INTERVAL_YEAR:
                break;
            case INTERVAL_YEAR_MONTH:
                break;
            case INTERVAL_MONTH:
                break;
            case INTERVAL_DAY:
                break;
            case INTERVAL_DAY_HOUR:
                break;
            case INTERVAL_DAY_MINUTE:
                break;
            case INTERVAL_DAY_SECOND:
                break;
            case INTERVAL_HOUR:
                break;
            case INTERVAL_HOUR_MINUTE:
                break;
            case INTERVAL_HOUR_SECOND:
                break;
            case INTERVAL_MINUTE:
                break;
            case INTERVAL_MINUTE_SECOND:
                break;
            case INTERVAL_SECOND:
                break;
            case CHAR:
            case VARCHAR:
                return ((NlsString) rexLiteral.getValue()).getValue();
            case BINARY:
            case VARBINARY:
                return ((org.apache.calcite.avatica.util.ByteString) rexLiteral.getValue()).getBytes();
            case NULL:
                return null;
            case ANY:
                break;
            case SYMBOL:
                break;
            case MULTISET:
                break;
            case ARRAY:
                break;
            case MAP:
                break;
            case DISTINCT:
                break;
            case STRUCTURED:
                break;
            case ROW:
                break;
            case OTHER:
                break;
            case CURSOR:
                break;
            case COLUMN_LIST:
                break;
            case DYNAMIC_STAR:
                break;
            case GEOMETRY:
                break;
        }
        throw new UnsupportedOperationException();
    }

    private List<FieldType> getFieldSchema(RelNode relNode) {
        RelDataType rowType = relNode.getRowType();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        ArrayList<FieldType> fieldSchemas = new ArrayList<>(fieldList.size());
        for (RelDataTypeField relDataTypeField : fieldList) {
            String name = relDataTypeField.getName();
            SqlTypeName sqlTypeName = relDataTypeField.getType().getSqlTypeName();
            fieldSchemas.add(fieldType(name, type(sqlTypeName)));
        }
        return fieldSchemas;
    }

    private String type(SqlTypeName type) {
        return QueryOp.typeMap.inverse().get(type);
    }

    //////////////////////////////////////////
    void visitFieldSchema(String id, String type) {

    }
}
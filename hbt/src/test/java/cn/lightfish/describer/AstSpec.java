package cn.lightfish.describer;

import cn.lightfish.DesRelNodeHandler;
import cn.lightfish.wu.BaseQuery;
import cn.lightfish.wu.ast.base.ExplainVisitor;
import cn.lightfish.wu.ast.base.Expr;
import cn.lightfish.wu.ast.base.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static cn.lightfish.DesRelNodeHandler.parse2SyntaxAst;

public class AstSpec extends BaseQuery {
    private static ExplainVisitor explainVisitor() {
        return new ExplainVisitor();
    }

    private ParseNode getParseNode(String text) {
        Describer describer = new Describer(text);
        return describer.expression();
    }

    @Test
    public void selectWithoutFrom() throws IOException {
        Schema select = valuesSchema(fields(fieldType("1", "int")), values());
        Assert.assertEquals("ValuesSchema(values=[], fieldNames=[FieldType(id=1, type=int)])", select.toString());
    }

    @Test
    public void selectWithoutFrom2() throws IOException {
        String text = "valuesSchema(fields(fieldType(id,int)),values())";
        ParseNode expression = getParseNode(text);
        Assert.assertEquals(text, expression.toString());
        String s = getS(expression);
        Assert.assertEquals("valuesSchema(fields(fieldType(id(\"id\"),id(\"int\"))),values())", s);


        Schema select = valuesSchema(fields(fieldType(id("id"), id("int"))), values());
        Assert.assertEquals("ValuesSchema(values=[], fieldNames=[FieldType(id=id, type=int)])", select.toString());
    }

    @Test
    public void selectAllWithoutFrom() throws IOException {
        Schema select = all(valuesSchema(fields(fieldType("1", "int")), values()));
        Assert.assertEquals("ValuesSchema(values=[], fieldNames=[FieldType(id=1, type=int)])", select.toString());
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
    }

    @Test
    public void selectDistinctWithoutFrom() throws IOException {
        Schema select = distinct(valuesSchema(fields(fieldType("1", "int")), values()));
        Assert.assertEquals("DistinctSchema(schema=ValuesSchema(values=[], fieldNames=[FieldType(id=1, type=int)]))", select.toString());
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
    public void selectProjectItemWithoutFrom() throws IOException {
        Schema select = projectNamed(valuesSchema(fields(fieldType("1", "int"), fieldType("2", "string")), values()), "2", "1");
        Assert.assertEquals("ProjectSchema(schema=ValuesSchema(values=[], fieldNames=[FieldType(id=1, type=int), FieldType(id=2, type=string)]), columnNames=[2, 1], fieldSchemaList=[FieldType(id=1, type=int), FieldType(id=2, type=string)])", select.toString());
    }

    @Test
    public void selectProjectItemWithoutFrom2() throws IOException {
        String text = "project(valuesSchema(fields(fieldType(id,int),fieldType(id2,int)),values()),id3,id4)";
        ParseNode expression = getParseNode(text);
        Assert.assertEquals(text, expression.toString());
        String s = getS(expression);
        Assert.assertEquals("project(valuesSchema(fields(fieldType(id(\"id\"),id(\"int\")),fieldType(id(\"id2\"),id(\"int\"))),values()),id(\"id3\"),id(\"id4\"))", s);

        Schema select = projectNamed(valuesSchema(fields(fieldType("id", "int"), fieldType("id2", "string")), values()), "id3", "id4");
        Assert.assertEquals("ProjectSchema(schema=ValuesSchema(values=[], fieldNames=[FieldType(id=id, type=int), FieldType(id=id2, type=string)]), columnNames=[id3, id4], fieldSchemaList=[FieldType(id=id, type=int), FieldType(id=id2, type=string)])", select.toString());
    }

    @Test
    public void from() throws IOException {
        String text = "from(db1,travelrecord)";
        ParseNode expression = getParseNode(text);
        Assert.assertEquals(text, expression.toString());
        String s = getS(expression);
        Assert.assertEquals("from(id(\"db1\"),id(\"travelrecord\"))", s);

        Assert.assertEquals("FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])", from(id("db1"), id("travelrecord")).toString());

        Schema select = from("db1", "travelrecord");
        Assert.assertEquals("FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])", select.toString());
    }

    @Test
    public void selectProjectFrom() throws IOException {
        String text = "from(db1,travelrecord).project(id)";
        String s = getS(parse2SyntaxAst(text));
        Assert.assertEquals("project(from(id(\"db1\"),id(\"travelrecord\")),id(\"id\"))", s);

        Schema select = projectNamed(from("db1", "travelrecord"), "1");
        Assert.assertEquals("ProjectSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), columnNames=[1], fieldSchemaList=[])", select.toString());
    }

    @Test
    public void selectUnionAll() throws IOException {
        Schema select = unionAll(from("db1", "travelrecord"), from("db1", "travelrecord"));
        Assert.assertEquals("SetOpSchema(op=UNION_ALL,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());

        String text = "from(db1,travelrecord) unionAll  from(\"db1\", \"travelrecord\")";
        String s = getS(parse2SyntaxAst(text));
        Assert.assertEquals("unionAll(from(id(\"db1\"),id(\"travelrecord\")),from(id(\"\"db1\"\"),id(\"\"travelrecord\"\")))", s);
    }

    @Test
    public void selectUnionDistinct() throws IOException {
        Schema select = unionDistinct(from("db1", "travelrecord"), from("db1", "travelrecord"));
        Assert.assertEquals("SetOpSchema(op=UNION_DISTINCT,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());

        String text = "from(db1,travelrecord) unionDistinct  from(\"db1\", \"travelrecord\")";
        Assert.assertEquals("unionDistinct(from(id(\"db1\"),id(\"travelrecord\")),from(id(\"\"db1\"\"),id(\"\"travelrecord\"\")))", getS(parse2SyntaxAst(text)));
    }

    private String getS(ParseNode parseNode) {
        return DesRelNodeHandler.syntaxAstToFlatSyntaxAstText(parseNode);
    }

    @Test
    public void selectExceptDistinct() throws IOException {
        Schema select = exceptDistinct(from("db1", "travelrecord"), from("db1", "travelrecord"));
        Assert.assertEquals("SetOpSchema(op=EXCEPT_DISTINCT,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());

        String text = "from(db1,travelrecord) exceptDistinct  from(\"db1\", \"travelrecord\")";
        Assert.assertEquals("exceptDistinct(from(id(\"db1\"),id(\"travelrecord\")),from(id(\"\"db1\"\"),id(\"\"travelrecord\"\")))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectExceptAll() throws IOException {
        Schema select = exceptAll(from("db1", "travelrecord"), from("db1", "travelrecord"));
        Assert.assertEquals("SetOpSchema(op=EXCEPT_ALL,list=[FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)])])", select.toString());

        String text = "from(db1,travelrecord) exceptAll  from(\"db1\", \"travelrecord\")";
        Assert.assertEquals("exceptAll(from(id(\"db1\"),id(\"travelrecord\")),from(id(\"\"db1\"\"),id(\"\"travelrecord\"\")))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectFromOrder() throws IOException {
        Schema schema = orderBy(from("db1", "travelrecord"), order("id", "ASC"), order("user_id", "DESC"));
        Assert.assertEquals("OrderSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), orders=[OrderItem(columnName=Identifier(value=id), direction=ASC), OrderItem(columnName=Identifier(value=user_id), direction=DESC)])", schema.toString());

        String text = "orderBy(from(db1,travelrecord),order(id,ASC), order(user_id,DESC))";
        Assert.assertEquals("orderBy(from(id(\"db1\"),id(\"travelrecord\")),order(id(\"id\"),id(\"asc\")),order(id(\"user_id\"),id(\"desc\")))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectFromLimit() throws IOException {
        Schema schema = limit(from("db1", "travelrecord"), 0, 1000);
        Assert.assertEquals("LimitSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), offset=Literal(value=0), limit=Literal(value=1000))", schema.toString());

        String text = "limit(from(db1,travelrecord),order(id,ASC), order(user_id,DESC),0,1000)";
        Assert.assertEquals("limit(from(id(\"db1\"),id(\"travelrecord\")),order(id(\"id\"),id(\"asc\")),order(id(\"user_id\"),id(\"desc\")),literal(0),literal(1000))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectFromGroupByKey() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectFromGroupByKeyAvg() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(avg("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='avg', alias='avg(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(avg(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(avg(id(\"id\"))))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectFromGroupByKeyCount() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(count("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='count', alias='count(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(count(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(count(id(\"id\"))))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectFromGroupByKeyCountStar() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(count("*")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='count', alias='count(*)', operands=[Identifier(value=*)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(count(*)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(count(id(\"*\"))))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectFromGroupByKeyCountDistinct() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(countDistinct("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='countDistinct', alias='count(distinct id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(countDistinct(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(countDistinct(id(\"id\"))))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectFromGroupByKeyFirst() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(first("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='first', alias='first(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(first(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(first(id(\"id\"))))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectFromGroupByKeyLast() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(last("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='last', alias='last(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(last(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(last(id(\"id\"))))", getS(parse2SyntaxAst(text)));
    }

    @Test
    public void selectFromGroupByKeyMax() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(max("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='max', alias='max(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text = "group(from(db1,travelrecord),keys(regular(id)), aggregating(max(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(max(id(\"id\"))))", getS(parse2SyntaxAst(text)));

        String text2 = "from(db1,travelrecord).group(keys(regular(id)),aggregating(max(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(max(id(\"id\"))))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void selectFromGroupByKeyMin() throws IOException {
        Schema schema = group(from("db1", "travelrecord"), keys(regular(id("id"))), aggregating(min("id")));
        Assert.assertEquals("GroupSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), keys=[GroupItem(exprs=[Identifier(value=id)])], exprs=[AggregateCall(function='min', alias='min(id)', operands=[Identifier(value=id)]])", schema.toString());

        String text2 = "from(db1,travelrecord).group(keys(regular(id)),aggregating(min(id)))";
        Assert.assertEquals("group(from(id(\"db1\"),id(\"travelrecord\")),keys(regular(id(\"id\"))),aggregating(min(id(\"id\"))))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void selectUcaseFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), ucase("id"));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[ucase(Identifier(value=id))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(ucase(id))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),ucase(id(\"id\")))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void selectUpperFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), upper("id"));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[upper(Identifier(value=id))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(upper(id))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),upper(id(\"id\")))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void selectLcaseFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), lcase("id"));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[lcase(Identifier(value=id))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(lcase(id))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),lcase(id(\"id\")))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void selectLowerFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), lower("id"));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[lower(Identifier(value=id))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(lower(id))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),lower(id(\"id\")))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void selectMidFrom() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), mid("id", 1));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[mid(Identifier(value=id),Literal(value=1))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(mid(id))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),mid(id(\"id\")))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void selectMidFrom2() throws IOException {
        Schema schema = map(from("db1", "travelrecord"), mid("id", 1, 3));
        Assert.assertEquals("MapSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), expr=[mid(Identifier(value=id),Literal(value=1),Literal(value=3))])", schema.toString());

        String text2 = "from(db1,travelrecord).map(mid(id,1,3))";
        Assert.assertEquals("map(from(id(\"db1\"),id(\"travelrecord\")),mid(id(\"id\"),literal(1),literal(3)))", getS(parse2SyntaxAst(text2)));
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
        Schema schema = filter(from("db1", "travelrecord"), in("id", 1, 2));
        Assert.assertEquals("FilterSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), exprs=[or(eq(Identifier(value=id),Literal(value=1)),eq(Identifier(value=id),Literal(value=2)))])", schema.toString());

        String text2 = "from(db1,travelrecord).filter(in(id,1,2))";
        Assert.assertEquals("filter(from(id(\"db1\"),id(\"travelrecord\")),in(id(\"id\"),literal(1),literal(2)))", getS(parse2SyntaxAst(text2)));
    }


    @Test
    public void filterBetween() throws IOException {
        Schema schema = filter(from("db1", "travelrecord"), between("id", 1, 2));
        Assert.assertEquals("FilterSchema(schema=FromSchema(names=[Identifier(value=db1), Identifier(value=travelrecord)]), exprs=[and(gte(Identifier(value=id),Literal(value=1)),lte(Identifier(value=id),Literal(value=2)))])", schema.toString());


        String text2 = "from(db1,travelrecord).filter(between(id,1,2))";
        Assert.assertEquals("filter(from(id(\"db1\"),id(\"travelrecord\")),between(id(\"id\"),literal(1),literal(2)))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testIsnull() throws IOException {
        Expr expr = isnull("id");
        Assert.assertEquals("isnull(Identifier(value=id))", expr.toString());

        String text2 = "isnull(id)";
        Assert.assertEquals("isnull(id(\"id\"))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testIfnull() throws IOException {
        Expr expr = ifnull("id", "default");
        Assert.assertEquals("ifnull(Identifier(value=id),Literal(value=default))", expr.toString());

        String text2 = "ifnull(id)";
        Assert.assertEquals("ifnull(id(\"id\"))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testNullif() throws IOException {
        Expr expr = nullif("id", "default");
        Assert.assertEquals("nullif(Identifier(value=id),Literal(value=default))", expr.toString());

        String text2 = "nullif(id)";
        Assert.assertEquals("nullif(id(\"id\"))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testIsNotNull() throws IOException {
        Expr expr = isnotnull("id");
        Assert.assertEquals("isnotnull(Identifier(value=id))", expr.toString());

        String text2 = "isnotnull(id)";
        Assert.assertEquals("isnotnull(id(\"id\"))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testInteger() throws IOException {
        Expr expr = literal(1);
        Assert.assertEquals("Literal(value=1)", expr.toString());

        String text2 = "1";
        Assert.assertEquals("literal(1)", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testLong() throws IOException {
        Expr expr = literal(1L);
        Assert.assertEquals("Literal(value=1)", expr.toString());

        String text2 = "1";
        Assert.assertEquals("literal(1)", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testFloat() throws IOException {
        Expr expr = literal(Float.MAX_VALUE);
        Assert.assertEquals("Literal(value=3.4028234663852886E+38)", expr.toString());

        String text2 = String.valueOf(Float.MAX_VALUE);
        Assert.assertEquals("literal(3.4028235E+38)", getS(parse2SyntaxAst(text2)));
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
    }


    @Test
    public void testAdd() throws IOException {
        Expr expr = plus(id("id"), literal(1));
        Assert.assertEquals("plus(Identifier(value=id),Literal(value=1))", expr.toString());

        String text2 = "id+1";
        Assert.assertEquals("plus(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testMinus() throws IOException {
        Expr expr = minus(id("id"), literal(1));
        Assert.assertEquals("minus(Identifier(value=id),Literal(value=1))", expr.toString());

        String text2 = "id-1";
        Assert.assertEquals("minus(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testEqual() throws IOException {
        Expr expr = eq(id("id"), literal(1));
        Assert.assertEquals("eq(Identifier(value=id),Literal(value=1))", expr.toString());

        String text2 = "id=1";
        Assert.assertEquals("eq(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testAnd() throws IOException {
        Expr expr = and(literal(1), literal(1));
        Assert.assertEquals("and(Literal(value=1),Literal(value=1))", expr.toString());

        String text2 = "1 and 1";
        Assert.assertEquals("and(literal(1),literal(1))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testOr() throws IOException {
        Expr expr = or(literal(1), literal(1));
        Assert.assertEquals("or(Literal(value=1),Literal(value=1))", expr.toString());

        String text2 = "1 or 1";
        Assert.assertEquals("or(literal(1),literal(1))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testAndOrPlus() throws IOException {
        String text2 = "1 or 1 + 1";
        Assert.assertEquals("or(literal(1),plus(literal(1),literal(1)))", getS(parse2SyntaxAst(text2)));
    }
    @Test
    public void testNot() throws IOException {
        Expr expr = not(literal(1));
        Assert.assertEquals("not(Literal(value=1))", expr.toString());

        String text2 = "not(1)";
        Assert.assertEquals("not(literal(1))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testNotEqual() throws IOException {
        Expr expr = ne(id("id"), literal(1));
        Assert.assertEquals("ne(Identifier(value=id),Literal(value=1))", expr.toString());

        String text2 = "id <> 1";
        Assert.assertEquals("ne(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testGreaterThan() throws IOException {
        Expr expr = gt(id("id"), literal(1));
        Assert.assertEquals("gt(Identifier(value=id),Literal(value=1))", expr.toString());

        String text2 = "id > 1";
        Assert.assertEquals("gt(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testGreaterThanEqual() throws IOException {
        Expr expr = gte(id("id"), literal(true));
        Assert.assertEquals("gte(Identifier(value=id),Literal(value=true))", expr.toString());

        String text2 = "id >= 1";
        Assert.assertEquals("gte(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testLessThan() throws IOException {
        Expr expr = lt(id("id"), literal(true));
        Assert.assertEquals("lt(Identifier(value=id),Literal(value=true))", expr.toString());

        String text2 = "id < 1";
        Assert.assertEquals("lt(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testLessThanEqual() throws IOException {
        Expr expr = lte(id("id"), literal(true));
        Assert.assertEquals("lte(Identifier(value=id),Literal(value=true))", expr.toString());

        String text2 = "id <= 1";
        Assert.assertEquals("lte(id(\"id\"),literal(1))", getS(parse2SyntaxAst(text2)));
    }


    @Test
    public void testAsColumnName() throws IOException {
        Expr expr = as(literal(1), id("column"));
        Assert.assertEquals("asColumnName(Literal(value=1),Identifier(value=column))", expr.toString());

        String text2 = "1 as column";
        Assert.assertEquals("as(literal(1),id(\"column\"))", getS(parse2SyntaxAst(text2)));
    }



    @Test
    public void testCast() throws IOException {
        Expr expr = cast(literal(1), id("float"));
        Assert.assertEquals("cast(Literal(value=1),Identifier(value=float))", expr.toString());

        String text2 = "cast(1,float)";
        Assert.assertEquals("cast(literal(1),id(\"float\"))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testInnerJoin() throws IOException {
        Schema schema = innerJoin(eq(id("id"), id("id")), from("db1", "table"), from("db1", "table2"));
        Assert.assertEquals("JoinSchema(type=INNER_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=table)]), FromSchema(names=[Identifier(value=db1), Identifier(value=table2)])], condition=eq(Identifier(value=id),Identifier(value=id)))", schema.toString());

        String text2 = "innerJoin(table.id = table2.id , from(db1,table),from(db1,table2))";
        Assert.assertEquals("innerJoin(eq(dot(id(\"table\"),id(\"id\")),dot(id(\"table2\"),id(\"id\"))),from(id(\"db1\"),id(\"table\")),from(id(\"db1\"),id(\"table2\")))", getS(parse2SyntaxAst(text2)));
    }

    @Test
    public void testLeftJoin() throws IOException {
        Schema schema = leftJoin(eq(id("id"), id("id")), from("db1", "table"), from("db1", "table2"));
        Assert.assertEquals("JoinSchema(type=LEFT_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=table)]), FromSchema(names=[Identifier(value=db1), Identifier(value=table2)])], condition=eq(Identifier(value=id),Identifier(value=id)))", schema.toString());
    }

    @Test
    public void testRightJoin() throws IOException {
        Schema schema = rightJoin(eq(id("id"), id("id")), from("db1", "table"), from("db1", "table2"));
        Assert.assertEquals("JoinSchema(type=RIGHT_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=table)]), FromSchema(names=[Identifier(value=db1), Identifier(value=table2)])], condition=eq(Identifier(value=id),Identifier(value=id)))", schema.toString());
    }

    @Test
    public void testFullJoin() throws IOException {
        Schema schema = fullJoin(eq(id("id"), id("id")), from("db1", "table"), from("db1", "table2"));
        Assert.assertEquals("JoinSchema(type=FULL_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=table)]), FromSchema(names=[Identifier(value=db1), Identifier(value=table2)])], condition=eq(Identifier(value=id),Identifier(value=id)))", schema.toString());
    }

    @Test
    public void testSemiJoin() throws IOException {
        Schema schema = semiJoin(eq(id("id"), id("id")), from("db1", "table"), from("db1", "table2"));
        Assert.assertEquals("JoinSchema(type=SEMI_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=table)]), FromSchema(names=[Identifier(value=db1), Identifier(value=table2)])], condition=eq(Identifier(value=id),Identifier(value=id)))", schema.toString());
    }

    @Test
    public void testAntiJoin() throws IOException {
        Schema schema = antiJoin(eq(id("id"), id("id")), from("db1", "table"), from("db1", "table2"));
        Assert.assertEquals("JoinSchema(type=ANTI_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=table)]), FromSchema(names=[Identifier(value=db1), Identifier(value=table2)])], condition=eq(Identifier(value=id),Identifier(value=id)))", schema.toString());
    }
//
//    @Test
//    public void testCorrelateInnerJoin() throws IOException {
//        Schema schema = correlateInnerJoin(eq(id( "id"), id("id")), from("db1", "table"), from("db1", "table2"));
//        Assert.assertEquals("JoinSchema(type=CORRELATE_INNER_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=table)]), FromSchema(names=[Identifier(value=db1), Identifier(value=table2)])], condition=eq(dot(Identifier(value=table),Identifier(value=id)),dot(Identifier(value=table2),Identifier(value=id))))", schema.toString());
//    }

//    @Test
//    public void testCorrelateLeftJoin() throws IOException {
//        Schema schema = correlateLeftJoin(eq(id("id"), id("id")), from("db1", "table"), from("db1", "table2"));
//        Assert.assertEquals("JoinSchema(type=CORRELATE_LEFT_JOIN, schemas=[FromSchema(names=[Identifier(value=db1), Identifier(value=table)]), FromSchema(names=[Identifier(value=db1), Identifier(value=table2)])], condition=eq(Identifier(value=id),Identifier(value=id)))", schema.toString());
//    }

}
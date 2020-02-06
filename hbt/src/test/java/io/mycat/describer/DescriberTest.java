package io.mycat.describer;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class DescriberTest {


    public void test() throws IOException {
        Describer describer = new Describer(new String(Files.readAllBytes(Paths.get("D:\\git\\describer\\src\\test\\java\\resources\\expr.des"))));
        describer.addOperator("+", 1, true);
        List<ParseNode> list = describer.statementList();
        Assert.assertEquals("let main = +(+(1,2),2);", list.get(0).toString());
    }
//    @Test
//    public void selectWithoutFrom() throws IOException {
//        String text = "valuesSchema(fields(fieldType(id,int)),values())";
//        ParseNode expression = getParseNode(text);
//        Assert.assertEquals(text, expression.toString());
//        String s = DesRelNodeHandler.syntaxAstToFlatSyntaxAstText(expression);
//        Assert.assertEquals("valuesSchema(fields(fieldType(id(\"id\"),id(\"int\"))),values())",s);
//    }


    //    @Test
//    public void selectAllWithoutFrom() throws IOException {
//        Schema select = all(valuesSchema(fields(fieldType("1", "int")), values()));
//        Assert.assertEquals("ValuesSchema(values=[], fieldNames=[FieldSchema(id=1, type=int)])", select.toString());
//    }
//    @Test
    public void test2() throws IOException {
        Describer describer = new Describer(new String(Files.readAllBytes(Paths.get("D:\\git\\describer\\src\\test\\java\\resources\\builder.des"))));
        describer.addOperator(".", "DOT", 16, true);
        describer.addOperator("DOT", 16, true);
        describer.addOperator("JOIN", 1, true);
        describer.addOperator("ON", 1, true);
        describer.addOperator("AS_COLUMNNAME", 1, true);
        describer.addOperator("=", "EQ", 15, true);
        describer.addOperator("EQ", 15, true);
        describer.addOperator("WHERE", 1, true);
        describer.addOperator("PROJECT", 1, true);
        describer.addOperator("OR", 1, true);
        describer.addOperator("AND", 1, true);
        describer.addOperator("FILTER", 1, true);
        describer.addOperator("MAP", 1, true);
        describer.addOperator("+", 1, true);
        List<ParseNode> list = describer.statementList();


        Assert.assertEquals("let main = DOT(DOT(join(as(travelrecord,t),as(address,a),EQ(DOT(t,id),DOT(a,id))),filter(or(EQ(DOT(t,id),1),EQ(DOT(a,id),2)))),map(DOT(t,id),DOT(t,user_id)));", list.get(0).toString());
    }
}
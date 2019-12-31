package io.mycat.describer;

import io.mycat.wu.BaseQuery;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

public class Test {
    public static void main(String[] args) throws CompileException {
        ExpressionEvaluator ee = new ExpressionEvaluator();
        ee.setDefaultImports("cn.lightfish.wu.Ast");
        ee.setExtendedClass(BaseQuery.class);
        ee.cook(" Schema map = map(filter((as(from(\"db1\", \"travelrecord\"), \"t\")), or(eq(dot(\"t\", \"id\"), new Literal(1)), eq(dot(\"a\", \"id\"), new Literal(2)))), dot(\"t\", \"id\"));\n");
    }
}
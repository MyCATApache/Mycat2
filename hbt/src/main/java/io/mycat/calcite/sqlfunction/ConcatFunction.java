package io.mycat.calcite.sqlfunction;

import lombok.SneakyThrows;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.codehaus.janino.ExpressionEvaluator;

import java.lang.reflect.Method;

public  class ConcatFunction {
        public static String eval(String... args) {
            return String.join("", args);
        }

        @SneakyThrows
    public static void main(String[] args) {
        Method[] methods = ConcatFunction.class.getMethods();
        MethodCallExpression call = Expressions.call(methods[1], Expressions.constant("1"), Expressions.constant("2"));
        System.out.println(call.toString());
        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
        expressionEvaluator.cook(call.toString());
            Object evaluate = expressionEvaluator.evaluate(new Object[]{});
            System.out.println();
        }
    }
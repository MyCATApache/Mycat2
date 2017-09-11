package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.byteArrayInterface.expr.ExprSQLParser;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jamie on 2017/9/6.
 */
public class ExprSQLParseTest extends TestCase {

    /**
     *
     simple_expr:
     literal
     | identifier
     | function_call
     | simple_expr COLLATE collation_name
     | param_marker
     | variable
     | simple_expr || simple_expr
     | + simple_expr
     | - simple_expr
     | ~ simple_expr
     | ! simple_expr
     | BINARY simple_expr
     | (expr [, expr] ...)
     | ROW (expr, expr [, expr] ...)
     | (subquery)
     | EXISTS (subquery)
     | {identifier expr}
     | match_expr
     | case_expr
     | interval_expr
     */
    @Test
    public void testExpr() throws Exception {
        String t = "'good'<>'good'";
        test(t);
    }
    @Test
    public void testExpr2() throws Exception {
        String t = "1<=2";
        test(t);
    }
    @Test
    public void testExpr3() throws Exception {
        String t = "NULL<=NULL";
        test(t);
    }
    @Test
    public void testExpr4() throws Exception {
        String t = "id between xx and xx";
        test(t);
    }
    @Test
    public void testSimpleExpr_literal() throws Exception {
        String t = "'a'";//literal
        test(t);
    }
    @Test
    public void testSimpleExpr_identifier() throws Exception {
        String t = "abc";//literal
        test(t);
    }
    @Test
    public void testSimpleExpr_function_call() throws Exception {
        String t = "call fun(a,c)";//function_call todo 语法是否正确
        test(t);
    }
    @Test
    public void testSimpleExpr_function_call2() throws Exception {
        String t = "fun(a,c)";//function_call todo 语法是否正确
        test(t);
    }
    @Test
    public void testSimpleExpr_COLLATE() throws Exception {
        String t = "(fun(a,c) COLLATE latin1_swedish_ci)";//simple_expr COLLATE collation_name
        test(t);
    }
    @Test
    public void testSimpleExpr_param_marker() throws Exception {
        String t = "?";//param_marker todo 语法是否正确
        test(t);
    }
    @Test
    public void testSimpleExpr_DECLARE_var_name () throws Exception {
        String t = "DECLARE";//todo 未完成 DECLARE var_name [, var_name] ... type [DEFAULT value] todo 语法是否正确
        test(t);
    }
    @Test
    public void testSimpleExpr_logic_or () throws Exception {
        String t = "fun2(v,c) || fun3(v1,c2)";//simple_expr || simple_expr
        test(t);
    }
    @Test
    public void testSimpleExpr_add () throws Exception {
        String t = "+ +fun2(v,c)";//+ simple_expr
        test(t);
    }
    @Test
    public void testSimpleExpr_minus () throws Exception {
        String t = "-fun2(v,c)";//+ simple_expr
        test(t);
    }
    @Test
    public void testSimpleExpr_tober () throws Exception {
        String t = "~fun2(v,c)";//~ simple_expr
        test(t);
    }
    @Test
    public void testSimpleExpr_colon () throws Exception {
        String t = "!fun2(v,c)";//! simple_expr
        test(t);
    }
    @Test
    public void testSimpleExpr_binary () throws Exception {
        String t = "BINARY fun2(v,c)";//BINARY simple_expr
        test(t);
    }
    @Test
    public void testSimpleExpr_multiple_expr () throws Exception {
        String t = "(!fun2(v,c),fun3(v3,c3),'qqq')";//(expr [, expr] ...)
        test(t);
    }
    @Test
    public void testSimpleExpr_row () throws Exception {
        String t = "row (!fun2(v,c),fun3(v3,c3),'qqq')";//ROW (expr, expr [, expr] ...)
        test(t);
    }
    @Test
    public void testSimpleExpr_subquery () throws Exception {
        String t = "(select 'good'<>'good')";//(subquery) //todo 没有完成,子查询得嵌入之前写的Select
        test(t);
    }
    @Test
    public void testSimpleExpr_exists () throws Exception {
        String t = "EXISTS (select 'good'<>'good')";//EXISTS (subquery)
        test(t);
    }
    @Test
    public void testSimpleExpr_identifier_expr () throws Exception {
        String t = "{identifier 1<=2}";//{identifier expr}
        test(t);
    }
    @Test
    public void testSimpleExpr_match_expr () throws Exception {
        String t = "identifier (select 'good'<>'good')";//todo match_expr //未实现
        test(t);
    }
    @Test
    public void testSimpleExpr_case_expr () throws Exception {
        String t = "identifier (select 'good'<>'good')";//todo case_expr //未实现
        test(t);
    }
    @Test
    public void testSimpleExpr_interval_expr () throws Exception {
        String t = "INTERVAL fun2(v,c) WEEK";//INTERVAL expr unit
        test(t);
    }
    /*
     bit_expr:
        bit_expr | bit_expr
      | bit_expr & bit_expr
      | bit_expr << bit_expr
      | bit_expr >> bit_expr
      | bit_expr + bit_expr
      | bit_expr - bit_expr
      | bit_expr * bit_expr
      | bit_expr / bit_expr
      | bit_expr DIV bit_expr
      | bit_expr MOD bit_expr
      | bit_expr % bit_expr
      | bit_expr ^ bit_expr
      | bit_expr + interval_expr
      | bit_expr - interval_expr
      | simple_expr

     */
    @Test
    public void testBit_expr1 () throws Exception {
        String t = "1 | 1";//
        test(t);
    }
    @Test
    public void testBit_expr2 () throws Exception {
        String t = "1 | 1";//
        test(t);
    }
    @Test
    public void testBit_expr3 () throws Exception {
        String t = "1 & 1";//
        test(t);
    }
    @Test
    public void testBit_expr4 () throws Exception {
        String t = "1 << 1";//
        test(t);
    }
    @Test
    public void testBit_expr5 () throws Exception {
        String t = "1 >> 1";//
    }
    @Test
    public void testBit_expr6 () throws Exception {
        String t = "1 >> 1";//
        test(t);
    }
    @Test
    public void testBit_expr7 () throws Exception {
        String t = "1 + 1";//
        test(t);
    }
    @Test
    public void testBit_expr8 () throws Exception {
        String t = "1 - 1";//
        test(t);
    }
    @Test
    public void testBit_expr9 () throws Exception {
        String t = "1 * 1";//
        test(t);
    }
    @Test
    public void testBit_expr10 () throws Exception {
        String t = "1 / 1";//todo 词法分析的bug
        test(t);
    }
    @Test
    public void testBit_expr11() throws Exception {
        String t = "1 DIV 1";//
        test(t);
    }

    @Test
    public void testBit_expr14 () throws Exception {
        String t = "1 MOD 1";//
        test(t);
    }
        @Test
    public void testBit_expr15() throws Exception {
        String t = "1 % 1";//
        test(t);
    }
    @Test
    public void testBit_expr16() throws Exception {
        String t = "1 ^ 1";//
        test(t);
    }
    @Test
    public void testBit_expr17 () throws Exception {
        String t = "1 + INTERVAL fun2(v,c) WEEK";//
        test(t);
    }
    @Test
    public void testBit_expr18 () throws Exception {
        String t = "1 - INTERVAL fun2(v,c) WEEK";//
        test(t);
    }

    /**
     predicate:
     bit_expr [NOT] IN (subquery)
     | bit_expr [NOT] IN (expr [, expr] ...)
     | bit_expr [NOT] BETWEEN bit_expr AND predicate
     | bit_expr SOUNDS LIKE bit_expr
     | bit_expr [NOT] LIKE simple_expr [ESCAPE simple_expr]
     | bit_expr [NOT] REGEXP bit_expr
     | bit_expr
     */
    /**
     bit_expr [NOT] IN (subquery)
     */
    @Test
    public void testPredicate () throws Exception {
        String t = "(identifier ) NOT IN (select 'good'<>'good')";//todo 没有实现子查询
        test(t);
    }

    /**
     bit_expr [NOT] IN (expr [, expr] ...)
     */
    @Test
    public void testPredicate2() throws Exception {
        String t = " 'good'+'good' NOT IN (( 'good'+'good'),1,1+2)";//
        test(t);
    }

    /**
     bit_expr [NOT] BETWEEN bit_expr AND predicate
     */
    @Test
    public void testPredicate3() throws Exception {
        String t = "( ('good'+'good')) NOT BETWEEN 1  AND 2";//
        test(t);
    }

    /**
     *
     bit_expr SOUNDS LIKE bit_expr
     */
    @Test
    public void testPredicate4() throws Exception {
        String t = "( ( 'good'+'good')) NOT REGEXP 1+2";//
        test(t);
    }

    /**
     *
     bit_expr [NOT] LIKE simple_expr [ESCAPE simple_expr]
     */
    @Test
    public void testPredicate5() throws Exception {
        String t = "( ( 'good'+'good')) NOT LIKE (1 + 1) ESCAPE (1 + 1)";//
        test(t);
    }
    @Test
    public void testPredicate6() throws Exception {
        String t = "( ( 'good'+'good')) REGEXP (1 + 1)";//
        test(t);
    }

    /**
     boolean_primary:
     boolean_primary IS [NOT] NULL
     | boolean_primary <=> predicate
     | boolean_primary comparison_operator predicate
     | boolean_primary comparison_operator {ALL | ANY} (subquery)
     | predicate
     */
    /*
              boolean_primary IS [NOT] NULL
     */
    @Test
    public void testBooleanPrimary() throws Exception {
        String t = "( ( 'good'+'good')) IS NOT NULL";//
        test(t);
    }
    /*
    boolean_primary <=> predicate
     */
    @Test
    public void testBooleanPrimary_predicate() throws Exception {
        String t = "( ( 'good'+'good')) <=> (1+1)";//
        test(t);
    }
    /*
    boolean_primary <=> predicate
     */
    @Test
    public void testBooleanPrimary_comparison_operator_predicate() throws Exception {
        String t = "( ( 'good'+'good'))!= (1+1)";//
        test(t);
    }
    /*
  boolean_primary comparison_operator {ALL | ANY} (subquery)
     */
    @Test
    public void testBooleanPrimary_comparison_operator_allany() throws Exception {
        String t = "( ( 'good'+'good'))!= ALL (select 'good'<>'good')";//todo 子查询未完成
        test(t);
    }

    /**
     expr:
     expr OR expr
     | expr || expr
     | expr XOR expr
     | expr AND expr
     | expr && expr
     | NOT expr
     | ! expr
     | boolean_primary IS [NOT] {TRUE | FALSE | UNKNOWN}
     | boolean_primary
     */
      /*
    boolean_primary <=> predicate
     */
    @Test
    public void testExpr5() throws Exception {
        String t = "1 or 2";//
        test(t);
    }
    @Test
    public void testExpr6() throws Exception {
        String t = "1 || 2";//
        test(t);
    }
    @Test
    public void testExpr7() throws Exception {
        String t = "1 XOR 2";//
        test(t);
    }
    @Test
    public void testExpr8() throws Exception {
        String t = "1 AND 2";//
        test(t);
    }
    @Test
    public void testExpr9() throws Exception {
        String t = "1 && 2";//
        test(t);
    }
    @Test
    public void testExpr10() throws Exception {
        String t = "! 1";//
        test(t);
    }
    @Test
    public void testExpr10_1() throws Exception {
        String t = "!(! (1+1))";//
        test(t);
    }
    @Test
    public void testExpr10_2() throws Exception {
        String t = "! 1+1+2-4";//
        test(t);
    }
    @Test
    public void testExpr11() throws Exception {
        String t = "! 1";//
        test(t);
    }
    @Test
    public void testExpr12() throws Exception {
        String t = "! 1";//
        test(t);
    }
    @Test
    public void testExpr13() throws Exception {
        String t = "! 1";//
        test(t);
    }
    @Test
    public void testExpr14() throws Exception {
        String t = "('2'+'2')!= (1+1) IS NOT TRUE";//
        test(t);
    }
    //    SQLParser parser;
    BufferSQLParser parser;
    BufferSQLContext context;
    private static final Logger LOGGER = LoggerFactory.getLogger(TCLSQLParserTest.class);

    private void test(String t) {
        LOGGER.info(t);
        byte[] bytes = t.getBytes();
        parser.parse(bytes, context);
        ExprSQLParser.pickExpr(0,context.getHashArray().getCount(),context,context.getHashArray(),context.getBuffer());
    }

    @Before
    protected void setUp() throws Exception {
        parser = new BufferSQLParser();
        context = new BufferSQLContext();
        //parser.init();
        MatchMethodGenerator.initShrinkCharTbl();
    }
}

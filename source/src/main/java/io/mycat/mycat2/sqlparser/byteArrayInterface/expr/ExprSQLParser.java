package io.mycat.mycat2.sqlparser.byteArrayInterface.expr;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.TokenHash;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;
import io.mycat.mycat2.sqlparser.byteArrayInterface.Tokenizer2;
import io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil;

/**
 * Created by jamie on 2017/9/5.
 */
public class ExprSQLParser {
    /**
     * expr:
     * expr OR expr
     * | expr XOR expr
     * | expr AND expr
     * | expr || expr
     * | expr && expr
     * | NOT expr
     * | ! expr
     * | boolean_primary IS [NOT] {TRUE | FALSE | UNKNOWN}
     * | boolean_primary
     */
    public static int pickExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        pos = pickBooleanPrimary(pos, arrayCount, context, hashArray, sql);
        int type = hashArray.getType(pos);
        long longHash = hashArray.getHash(pos);
        if (Tokenizer2.OR == type) {
            //todo 如果词法分析改变了,|| 变为一个关键词,这里得改
            ++pos;
            type = hashArray.getType(pos);
            if (TokenHash.OR == type) {
                TokenizerUtil.debug(()->"expr || expr");
                //todo 如果词法分析改变了,|| 变为一个关键词,这里得改
                ++pos;
                return pickExpr(pos, arrayCount, context, hashArray, sql);
            }
        } else if (TokenHash.XOR == longHash) {
            TokenizerUtil.debug(()->"expr XOR expr");
            ++pos;
            return pickExpr(pos, arrayCount, context, hashArray, sql);
        } else if (TokenHash.AND == longHash) {
            TokenizerUtil.debug(()->"expr AND expr");
            ++pos;
            return pickExpr(pos, arrayCount, context, hashArray, sql);
        } else if (TokenHash.NOT == longHash) {
            TokenizerUtil.debug(()->"expr NOT expr");
            ++pos;
            return pickExpr(pos, arrayCount, context, hashArray, sql);
        } else {
            type = hashArray.getType(pos);
            if (Tokenizer2.COLON == type) {//!
                TokenizerUtil.debug(()->"! expr");
                ++pos;
                return pickExpr(pos, arrayCount, context, hashArray, sql);
            }
            //  pos = pickBooleanPrimary(pos, arrayCount, context, hashArray, sql);
            longHash = hashArray.getHash(pos);
            if (longHash == TokenHash.IS) {
                TokenizerUtil.debug(()->"IS");
                longHash = hashArray.getHash(++pos);
                if (longHash == TokenHash.NOT) {
                    TokenizerUtil.debug(()->"NOT");
                    longHash = hashArray.getHash(++pos);
                }
                if (longHash == TokenHash.TRUE || longHash == TokenHash.FALSE || longHash == TokenHash.UNKNOWN) {
                    //todo boolean_primary IS [NOT] {TRUE | FALSE | UNKNOWN}
                    TokenizerUtil.debug(pos, context);
                    ++pos;
                    return pos;
                }
                //语法错误
            } else {
                //todo boolean_primary
                //  pos = pickBooleanPrimary(pos, arrayCount, context, hashArray, sql);
            }
            //语法错误
        }

        return pos;
    }

    /**
     * boolean_primary:
     * boolean_primary IS [NOT] NULL
     * | boolean_primary <=> predicate
     * | boolean_primary comparison_operator predicate
     * | boolean_primary comparison_operator {ALL | ANY} (subquery)
     * | predicate
     */
    public static int pickBooleanPrimary(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        pos = pickPredicate(pos, arrayCount, context, hashArray, sql);
        TokenizerUtil.debug(pos, context);
        long longHash = hashArray.getHash(pos);
        if (TokenHash.IS == longHash) {
            TokenizerUtil.debug(pos, context);
            longHash = hashArray.getHash(++pos );
            if (TokenHash.NOT == longHash) {
                TokenizerUtil.debug(pos, context);
                ++pos;
                longHash = hashArray.getHash(pos);
            }
            if (TokenHash.NULL == longHash) {
                TokenizerUtil.debug(pos, context);
                ++pos;
                return pos;
            }else {
                TokenizerUtil.debug(()->"语法错误");
            }
        }else {
            int type = hashArray.getType(pos);
            TokenizerUtil.debug(pos, context);
            int t2 = hashArray.getType(pos + 1);
            TokenizerUtil.debug(pos+1, context);
            int t3 = hashArray.getType(pos + 2);
            TokenizerUtil.debug(pos+2, context);
            if (type == Tokenizer2.LESS && t2 == Tokenizer2.EQUAL && t3 == Tokenizer2.GREATER) {
                TokenizerUtil.debug(()->"boolean_primary <=> predicate");
                return pickPredicate(pos + 3, arrayCount, context, hashArray, sql);
            }
            if (ExprSQLParserHelper.isComparisonOperatorByType(type)) {
                pos = ExprSQLParserHelper.pickComparisonOperator(pos, arrayCount, context, hashArray, sql);
                longHash = hashArray.getHash(pos);
                TokenizerUtil.debug(pos, context);
                if (longHash == TokenHash.ALL) {
                    TokenizerUtil.debug(pos, context);
                    ++pos;
                } else if (longHash == TokenHash.ANY) {
                    TokenizerUtil.debug(pos, context);
                    ++pos;
                } else {
                    //todo boolean_primary comparison_operator predicate
                    return pickPredicate(pos, arrayCount, context, hashArray, sql);
                }
                int type3 = hashArray.getType(pos);
                if (Tokenizer2.LEFT_PARENTHESES == type3) {
                    ++pos;
                    pos = ExprSQLParserHelper.pickSubquery(pos, arrayCount, context, hashArray, sql);
                    int type4 = hashArray.getType(pos);
                    if (Tokenizer2.RIGHT_PARENTHESES == type4) {
                        ++pos;
                        return pos;
                    }
                }
            }
        }
        return pos;

    }

    /**
     * predicate:
     * | bit_expr SOUNDS LIKE bit_expr
     * | bit_expr [NOT] IN (subquery)
     * | bit_expr [NOT] IN (expr [, expr] ...)
     * | bit_expr [NOT] BETWEEN bit_expr AND predicate
     * | bit_expr [NOT] LIKE simple_expr [ESCAPE simple_expr]
     * | bit_expr [NOT] REGEXP bit_expr
     * | bit_expr
     */

    public static int pickPredicate(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        pos = pickBitExpr(pos, arrayCount, context, hashArray, sql);
        TokenizerUtil.debug(pos, context);
        long longHash = hashArray.getHash(pos);
//        if (Tokenizer2.RIGHT_PARENTHESES==hashArray.getType(pos)){
//            ++pos;
//        }
        if (TokenHash.SOUNDS == longHash) {
            longHash = hashArray.getHash(++pos);
            if (TokenHash.LIKE == longHash) {
                TokenizerUtil.debug(()->"bit_expr SOUNDS LIKE bit_expr");
                return pickBitExpr(++pos, arrayCount, context, hashArray, sql);
            }
        } else if (TokenHash.NOT == longHash) {
            //todo 捕获 NOT
            TokenizerUtil.debug(()->"NOT");
            ++pos;
            longHash = hashArray.getHash(pos);
        }

        if (TokenHash.IN == longHash) {
            ++pos;//todo 直接跳过括号
            if (ExprSQLParserHelper.isSubquery(pos+1, arrayCount, context, hashArray, sql)) {
                TokenizerUtil.debug(()->"IN (subquery)");
                pos= ExprSQLParserHelper.pickSubquery(++pos, arrayCount, context, hashArray, sql);
               int type = hashArray.getType(pos);
                if (type==Tokenizer2.RIGHT_PARENTHESES){
                    pos++;
                    return pos;
                }else {
                    TokenizerUtil.debug(()->"IN (subquery)语法错误");
                }
                return pos;
            } else {
                TokenizerUtil.debug(()->"IN (expr [, expr] ...)");
                ++pos;
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                int type = hashArray.getType(pos);
                while (type == Tokenizer2.COMMA) {
                    ++pos;
                    pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                    type = hashArray.getType(pos);
                }
                //todo bit_expr [NOT] IN (expr [, expr] ...)
                if (type==Tokenizer2.RIGHT_PARENTHESES){
                    pos++;
                    return pos;
                }else {
                    TokenizerUtil.debug(()->"IN (expr [, expr] ...) 语法错误");
                }

                //todo 直接跳过括号
                return pos;
            }
        } else if (TokenHash.BETWEEN == longHash) {
            TokenizerUtil.debug(() -> "BETWEEN bit_expr AND predicate");
            pos = pickBitExpr(++pos, arrayCount, context, hashArray, sql);
            longHash = hashArray.getHash(pos);
            if (longHash == TokenHash.AND) {
                TokenizerUtil.debug(() -> "AND");
                //todo bit_expr [NOT] BETWEEN bit_expr AND predicate
                return pickPredicate(++pos, arrayCount, context, hashArray, sql);
            }
        } else if (TokenHash.LIKE == longHash) {
            TokenizerUtil.debug(()->"LIKE simple_expr");
            ++pos;
            pos = pickSimpleExpr(pos, arrayCount, context, hashArray, sql);
            longHash = hashArray.getHash(pos);
            //todo bit_expr [NOT] LIKE simple_expr [ESCAPE simple_expr]
            if (longHash == TokenHash.ESCAPE) {
                TokenizerUtil.debug(()->"[ESCAPE simple_expr]");
                pos = pickSimpleExpr(++pos, arrayCount, context, hashArray, sql);
            }
        } else if (TokenHash.REGEXP == longHash) {
            //todo bit_expr [NOT] REGEXP bit_expr
            TokenizerUtil.debug(()->"REGEXP");
            ++pos;
            return pickBitExpr(++pos, arrayCount, context, hashArray, sql);
        }


        return pos;
    }

    /**
     * bit_expr:
     * bit_expr | bit_expr
     * | bit_expr & bit_expr
     * | bit_expr << bit_expr
     * | bit_expr >> bit_expr
     * | bit_expr + bit_expr
     * | bit_expr - bit_expr
     * | bit_expr * bit_expr
     * | bit_expr / bit_expr
     * | bit_expr DIV bit_expr
     * | bit_expr MOD bit_expr
     * | bit_expr % bit_expr
     * | bit_expr ^ bit_expr
     * | bit_expr + interval_expr
     * | bit_expr - interval_expr
     * | simple_expr
     */
    public static int pickBitExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        pos = pickSimpleExpr(pos, arrayCount, context, hashArray, sql);
        int type = hashArray.getType(pos);
        switch (type) {
            case Tokenizer2.OR: {
                ++pos;
                TokenizerUtil.debug(() -> "bit_expr | bit_expr");
                //todo  bit_expr | bit_expr
                return pickBitExpr(pos, arrayCount, context, hashArray, sql);
            }
            case Tokenizer2.LESS: {
                type = hashArray.getType(pos+1);
                if (Tokenizer2.LESS == type) {
                    pos+=2;
                    TokenizerUtil.debug(() -> "bit_expr << bit_expr");
                    //todo  bit_expr << bit_expr
                    return pickBitExpr(++pos, arrayCount, context, hashArray, sql);
                }else {
                    //todo maybe <=> boolean_primary <=> predicate
                    return pos;
                }
            }
            case Tokenizer2.GREATER: {
                ++pos;
                type = hashArray.getType(pos);
                if (Tokenizer2.GREATER == type) {
                    TokenizerUtil.debug(() -> "bit_expr >> bit_expr");
                    //todo  bit_expr >> bit_expr
                    return pickBitExpr(++pos, arrayCount, context, hashArray, sql);
                }
            }
            case Tokenizer2.PLUS: {
                ++pos;
                if (TokenHash.INTERVAL == hashArray.getHash(pos)) {
                    TokenizerUtil.debug(() -> "bit_expr + interval_expr");
                    return pickIntervalExpr(++pos, arrayCount, context, hashArray, sql);
                } else {
                    TokenizerUtil.debug(() -> "bit_expr + bit_expr");
                    //todo  bit_expr + bit_expr
                    return pickBitExpr(pos, arrayCount, context, hashArray, sql);
                }

            }
            case Tokenizer2.MINUS: {
                ++pos;
                if (TokenHash.INTERVAL == hashArray.getHash(pos)) {
                    TokenizerUtil.debug(() -> "bit_expr +- interval_expr");
                    return pickIntervalExpr(++pos, arrayCount, context, hashArray, sql);
                } else {
                    TokenizerUtil.debug(() -> "bit_expr - bit_expr");
                    //todo  bit_expr - bit_expr
                    return pickBitExpr(pos, arrayCount, context, hashArray, sql);
                }
            }
            case Tokenizer2.AND: {
                ++pos;
                TokenizerUtil.debug(() -> "bit_expr & bit_expr");
                //todo  bit_expr & bit_expr
                return pickBitExpr(pos, arrayCount, context, hashArray, sql);
            }
            case Tokenizer2.STAR: {
                ++pos;
                TokenizerUtil.debug(() -> "bit_expr * bit_expr");
                //todo  bit_expr * bit_expr
                return pickBitExpr(pos, arrayCount, context, hashArray, sql);
            }
            case Tokenizer2.DIVISION: {
                ++pos;
                TokenizerUtil.debug(() -> "bit_expr / bit_expr");
                //todo  bit_expr / bit_expr
                return pickBitExpr(pos, arrayCount, context, hashArray, sql);
            }
            //CARET  ^
            case Tokenizer2.CARET: {//^
                ++pos;
                TokenizerUtil.debug(() -> "bit_expr ^ bit_expr");
                //todo  bit_expr ^ bit_expr
                return pickBitExpr(pos, arrayCount, context, hashArray, sql);
            }
            case Tokenizer2.PERCENT: {
                ++pos;
                TokenizerUtil.debug(() -> "bit_expr % bit_expr");
                //todo  bit_expr % bit_expr
                return pickBitExpr(pos, arrayCount, context, hashArray, sql);
            }
            default: {
                long longHash = hashArray.getHash(pos);
                if (TokenHash.DIV == longHash) {
                    ++pos;
                    TokenizerUtil.debug(() -> "bit_expr DIV bit_expr");
                    //todo  bit_expr DIV bit_expr
                    return pickBitExpr(pos, arrayCount, context, hashArray, sql);
                } else if (TokenHash.MOD == longHash) {
                    ++pos;
                    TokenizerUtil.debug(() -> "bit_expr MOD bit_expr");
                    //todo  bit_expr MOD bit_expr
                    return pickBitExpr(pos, arrayCount, context, hashArray, sql);
                } else {
                    TokenizerUtil.debug(pos, context);
                }
            }
//            TokenizerUtil.debug(() -> "pickBitExpr:语法错误");
        }

        return pos;
    }

    public static int pickIdentifierExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        TokenizerUtil.debug(pos, context);
        ++pos;
        int type=hashArray.getType(pos);
        if (Tokenizer2.DOT==type){
            ++pos;
            TokenizerUtil.debug(pos, context);
        }
        return pos;
    }

    /**
     * 下面这个语法是调整过顺序的(或语义的顺序调整,对解析没有影响)
     * simple_expr:
     * | + simple_expr
     * | - simple_expr
     * | ~ simple_expr
     * | ! simple_expr
     * | param_marker  // ?
     * | (expr [, expr] ...)
     * | (subquery)
     * | BINARY simple_expr
     * | EXISTS (subquery)
     * | ROW (expr, expr [, expr] ...)
     * | match_expr  //MATCH 开头
     * | case_expr    //CASE开头
     * | variable //SET variable_assignment [, variable_assignment] ... SET开头
     * | function_call //CALL sp_name([parameter[,...]])
     * | literal
     * | identifier
     * | {identifier expr}
     * | interval_expr //represents a time interval. The syntax is INTERVAL expr unit, where unit is a specifier such as HOUR, DAY, or WEEK
     * | simple_expr COLLATE collation_name
     * | simple_expr || simple_expr
     */
    public static int pickSimpleExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        int type = hashArray.getType(pos);
        long longHash;
        switch (type) {
            case Tokenizer2.PLUS:// =
                TokenizerUtil.debug(() -> "+ simple_expr");
                ++pos;
                pos = pickSimpleExpr(pos, arrayCount, context, hashArray, sql);
                return pos;
            case Tokenizer2.MINUS: {// -
                TokenizerUtil.debug(() -> "- simple_expr");
                ++pos;
                pos = pickSimpleExpr(pos, arrayCount, context, hashArray, sql);
                return pos;
            }
            case Tokenizer2.TOBER: {// ~
                TokenizerUtil.debug(() -> "~ simple_expr");
                ++pos;
                pos = pickSimpleExpr(pos, arrayCount, context, hashArray, sql);
                return pos;
            }
            case Tokenizer2.COLON: {
                TokenizerUtil.debug(() -> "! simple_expr");
                ++pos;
                pos = pickSimpleExpr(pos, arrayCount, context, hashArray, sql);
                return pos;
            }
            case Tokenizer2.LEFT_PARENTHESES: {
                TokenizerUtil.debug(() -> "(expr [, expr] ...)| (subquery)");
                ++pos;//todo   | (expr [, expr] ...)| (subquery)
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                type = hashArray.getType(pos);
                while (type == Tokenizer2.COMMA) {
                    ++pos;
                    pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                    type = hashArray.getType(pos);
                }
                TokenizerUtil.debug(pos, context);
                if (Tokenizer2.RIGHT_PARENTHESES == hashArray.getType(pos)) {
                    ++pos;
                } else {
                    TokenizerUtil.debug(() -> "括号没有匹配");
                }
                return pos;
            }
            case Tokenizer2.QUESTION_MARK: {
                ++pos;
                TokenizerUtil.debug(() -> "param_marker//?");
                return pos;
            }
            case Tokenizer2.LEFT_CURLY_BRACKET: {
                TokenizerUtil.debug(() -> "{");
                ++pos;
                TokenizerUtil.debug(() -> "identifier expr");
                //todo 捕获 identifier
                TokenizerUtil.debug(pos, context);
                ++pos;
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                type = hashArray.getType(pos);
                if (type == Tokenizer2.RIGHT_CURLY_BRACKET) {
                    TokenizerUtil.debug(() -> "}");
                    ++pos;
                    return pos;
                } else {
                    TokenizerUtil.debug(() -> "括号不匹配");
                    return pos;
                }
            }
            default: {
                longHash = hashArray.getHash(pos);
                /**
                 * | BINARY simple_expr
                 * | EXISTS (subquery)
                 * | ROW (expr, expr [, expr] ...)
                 *| match_expr  //MATCH 开头
                 * | case_expr    //CASE开头
                 * todo 好多函数没有写
                 */
                if (TokenHash.BINARY == longHash) {
                    TokenizerUtil.debug(() -> "BINARY simple_expr");
                    ++pos;
                    pos = pickSimpleExpr(pos, arrayCount, context, hashArray, sql);
                } else if (TokenHash.EXISTS == longHash) {
                    TokenizerUtil.debug(() -> " EXISTS (subquery)");
                    ++pos;
                    pos = ExprSQLParserHelper.pickSubquery(pos, arrayCount, context, hashArray, sql);
                } else if (TokenHash.ROW == longHash) {
                    TokenizerUtil.debug(() -> "ROW (expr, expr [, expr] ...");
                    ++pos;
                    pos = pickRowExpr(pos, arrayCount, context, hashArray, sql);
                } else if (TokenHash.MATCH == longHash) {
                    TokenizerUtil.debug(() -> "match_expr  //MATCH 开头");
                    ++pos;
                    pos = pickMatchExpr(pos, arrayCount, context, hashArray, sql);
                } else if (TokenHash.CASE == longHash) {
                    TokenizerUtil.debug(() -> "case_expr   还没写完整 //CASE开头");
                    ++pos;
                    pos = pickCaseExpr(pos, arrayCount, context, hashArray, sql);
                } else if (TokenHash.SET == longHash) {
                    ++pos;
                    pos = pickVariableExpr(pos, arrayCount, context, hashArray, sql);
                } else if (TokenHash.CALL == longHash) {
                    TokenizerUtil.debug(() -> "存储过程调用");
                    ++pos;
                    pos = pickFunctionCall(pos, arrayCount, context, hashArray, sql);
                } else if (TokenHash.INTERVAL == longHash) {
                    TokenizerUtil.debug(() -> "INTERVAL expr unit");
                    ++pos;
                    return pickIntervalExpr(pos, arrayCount, context, hashArray, sql);
                } else {
                    TokenizerUtil.debug(() -> "literal或者identifier");
                    TokenizerUtil.debug(pos, context);
                    pos=pickIdentifierExpr(pos, arrayCount, context, hashArray, sql);
                }
                /**todo  捕获
                 * literal
                 * 或者
                 * identifier
                 * 根据上文判断类型
                 *
                 * interval_expr可以根据后文判断类型
                 *
                 */
                /**
                 * | literal
                 * | identifier
                 * | {identifier expr}
                 * | interval_expr //represents a time interval. The syntax is INTERVAL expr unit, where unit is a specifier such as HOUR, DAY, or WEEK
                 * | simple_expr COLLATE collation_name
                 * | simple_expr || simple_expr
                 */


                long longHash2Pos = hashArray.getHash(pos);//位置2
                int type2 = hashArray.getType(pos);
                type2 = hashArray.getType(pos);
                if (type2 == Tokenizer2.LEFT_PARENTHESES) {
                    //todo 逻辑优化
                    TokenizerUtil.debug(() -> "表达式函数调用");
                    pos = pickFunctionCall(--pos, arrayCount, context, hashArray, sql);
                    TokenizerUtil.debug(() -> "表达式函数调用结束尾部");
                    TokenizerUtil.debug(pos, context);
                    type2 = hashArray.getType(pos);
                }
                longHash2Pos = hashArray.getHash(pos);//位置2
                type2 = hashArray.getType(pos);
                if (longHash2Pos == TokenHash.COLLATE) {
                    TokenizerUtil.debug(() -> "simple_expr COLLATE collation_name");
                    //todo  捕获一个字符串
                    ++pos;
                    TokenizerUtil.debug(() -> "collation_name:");
                    TokenizerUtil.debug(pos, context);
                    ++pos;
                }
                longHash2Pos = hashArray.getHash(pos);//位置2
                type2 = hashArray.getType(pos);
                if (type2 == Tokenizer2.OR) {
                    //todo 这里两个OR 其实是||
                    type2 = hashArray.getType(pos + 1);
                    if (type2 == Tokenizer2.OR) {
                        pos += 2;
                        TokenizerUtil.debug(() -> "simple_expr || simple_expr");
                        //todo 捕获 simple_expr || simple_expr
                        return pickSimpleExpr(pos, arrayCount, context, hashArray, sql);

                    }
                    return pos;
                }

                //identifier expr
                //  TokenizerUtil.debug(()->"identifier expr");
                //  pos=pickExpr(pos, arrayCount, context, hashArray, sql);
                return pos;
                //语法错误
            }
        }
    }

    /**
     * interval_expr
     */
    public static int pickIntervalExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        pos = pickExpr(pos, arrayCount, context, hashArray, sql);
        //todo 捕获 unit
        TokenizerUtil.debug(() -> "unit:");
        TokenizerUtil.debug(pos, context);
        ++pos;
        return pos;
    }

    /**
     * ROW (expr, expr [, expr] ...)
     */
    public static int pickRowExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        int type = hashArray.getType(pos);
        if (type == Tokenizer2.LEFT_PARENTHESES) {
            ++pos;
            pos = pickExpr(pos, arrayCount, context, hashArray, sql);
            type = hashArray.getType(pos);
            while (Tokenizer2.COMMA == type) {
                ++pos;
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                type = hashArray.getType(pos);
            }
            type = hashArray.getType(pos);
            if (type == Tokenizer2.RIGHT_PARENTHESES) {
                ++pos;
                return pos;
            }
        }
        return pos;//语法错误
    }

    /**
     * MATCH (col1,col2,...) AGAINST (expr [search_modifier])
     */
    public static int pickMatchExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        int type = hashArray.getType(pos);
        if (type == Tokenizer2.LEFT_PARENTHESES) {
            ++pos;
            pos = pickExpr(pos, arrayCount, context, hashArray, sql);
            type = hashArray.getType(pos);//todo 改成字段匹配
            while (Tokenizer2.COMMA == type) {
                ++pos;
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                type = hashArray.getType(pos);
            }
            type = hashArray.getType(pos);
            if (type == Tokenizer2.RIGHT_PARENTHESES) {
                ++pos;
                return pos;
            }
        }
        long longHash = hashArray.getHash(pos);
        if (longHash == TokenHash.AGAINST) {
            ++pos;
            if (type == Tokenizer2.LEFT_PARENTHESES) {
                ++pos;
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                pos = ExprSQLParserHelper.pickSearchModifier(pos, arrayCount, context, hashArray, sql);
                type = hashArray.getType(pos);
                if (type == Tokenizer2.RIGHT_PARENTHESES) {
                    ++pos;
                    return pos;
                }
            }
        } else {
            //语法错误
        }
        return pos;//语法错误
    }

    /**
     * WHEN [compare_value] THEN result
     */
    public static int pickWhenExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        pos = pickExpr(pos, arrayCount, context, hashArray, sql);
        long longHash = hashArray.getHash(pos);
        if (longHash == TokenHash.THEN) {
            pos = pickExpr(pos, arrayCount, context, hashArray, sql);
        } else {
            //语法错误
        }
        return pos;
    }

    //
//    public static boolean isWhen(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
//        long longHash = hashArray.getHash(pos);
//        if (longHash == TokenHash.WHEN) {
//            return true;
//        }
//        return false;
//    }
    public static boolean is(long tokenHash, int pos, HashArray hashArray) {
        long longHash = hashArray.getHash(pos);
        if (longHash == TokenHash.ELSE) {
            return true;
        }
        return false;
    }

    /**
     * @param pos
     * @param arrayCount
     * @param context
     * @param hashArray
     * @param sql
     * @return
     */
    public static int pickCaseExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.CASE == longHash) {
            ++pos;
            if (is(TokenHash.WHEN, pos, hashArray)) {
                /*
                 CASE WHEN [condition] THEN result [WHEN [condition] THEN result ...] [ELSE result] END
          */
                pos = pickWhenExpr(pos, arrayCount, context, hashArray, sql);
                while (is(TokenHash.WHEN, pos, hashArray)) {
                    pos = pickWhenExpr(pos, arrayCount, context, hashArray, sql);
                    if (is(TokenHash.ELSE, pos, hashArray)) {
                        ++pos;
                        //捕获 result
                        ++pos;
                    }
                    longHash = hashArray.getHash(pos);
                    if (TokenHash.END == longHash) {
                        ++pos;
                        return pos;
                    }
                }

            } else {
                //todo 捕获 value
                ++pos;
            /*
            CASE value WHEN [compare_value] THEN result [WHEN [compare_value] THEN result ...] [ELSE result] END

            */
                pos = pickWhenExpr(pos, arrayCount, context, hashArray, sql);
                while (is(TokenHash.WHEN, pos, hashArray)) {
                    pos = pickWhenExpr(pos, arrayCount, context, hashArray, sql);
                    if (is(TokenHash.ELSE, pos, hashArray)) {
                        ++pos;
                        //捕获 result
                        ++pos;
                    }
                    longHash = hashArray.getHash(pos);
                    if (TokenHash.END == longHash) {
                        ++pos;
                        return pos;
                    }
                }
            }
        } else if (TokenHash.IF == longHash) {
            /**
             * IF(expr1,expr2,expr3)
             */
            int type = hashArray.getType(++pos);
            ;
            if (type == Tokenizer2.LEFT_PARENTHESES) {
                ++pos;
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                type = hashArray.getType(pos);
                int count = 0;
                while (Tokenizer2.COMMA == type && count < 3) {
                    ++pos;
                    pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                    type = hashArray.getType(pos);
                    count++;
                }
                type = hashArray.getType(pos);
                if (type == Tokenizer2.RIGHT_PARENTHESES) {
                    ++pos;
                    return pos;
                }
            }
        } else if (TokenHash.IFNULL == longHash) {
            //todo 后期生成 TokenHash.IFNULL
            /**
             * IFNULL(expr1,expr2)
             */
            int type = hashArray.getType(++pos);
            ;
            if (type == Tokenizer2.LEFT_PARENTHESES) {
                ++pos;
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                type = hashArray.getType(pos);
                int count = 0;
                while (Tokenizer2.COMMA == type && count < 2) {
                    ++pos;
                    pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                    type = hashArray.getType(pos);
                    count++;
                }
                type = hashArray.getType(pos);
                if (type == Tokenizer2.RIGHT_PARENTHESES) {
                    ++pos;
                    return pos;
                }
            }
        } else if (TokenHash.NULLIF == longHash) {
            /**
             * NULLIF(expr1,expr2)
             */
            int type = hashArray.getType(++pos);
            ;
            if (type == Tokenizer2.LEFT_PARENTHESES) {
                ++pos;
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                type = hashArray.getType(pos);
                int count = 0;
                while (Tokenizer2.COMMA == type && count < 2) {
                    ++pos;
                    pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                    type = hashArray.getType(pos);
                    count++;
                }
                type = hashArray.getType(pos);
                if (type == Tokenizer2.RIGHT_PARENTHESES) {
                    ++pos;
                    return pos;
                }
            }
        }
        return pos;//语法错误
    }

    /**
     * variable //SET variable_assignment [, variable_assignment] ... SET开头
     * 不是重要任务,先不写
     *
     * @param pos
     * @param arrayCount
     * @param context
     * @param hashArray
     * @param sql
     * @return
     */
    public static int pickVariableExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        int type = hashArray.getType(pos);
        if (type == Tokenizer2.LEFT_PARENTHESES) {
            ++pos;
            pos = pickExpr(pos, arrayCount, context, hashArray, sql);
            type = hashArray.getType(pos);
            while (Tokenizer2.COMMA == type) {
                ++pos;
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                type = hashArray.getType(pos);
            }
            type = hashArray.getType(pos);
            if (type == Tokenizer2.RIGHT_PARENTHESES) {
                ++pos;
                return pos;
            }
        }
        return pos;//语法错误
    }

    public static int pickFunctionCall(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        TokenizerUtil.debug(pos, context);
        ++pos;
        int type = hashArray.getType(pos);
        if (type == Tokenizer2.LEFT_PARENTHESES) {
            TokenizerUtil.debug(() -> "(");
            ++pos;
            TokenizerUtil.debug(pos, context);
            pos = pickExpr(pos, arrayCount, context, hashArray, sql);
            type = hashArray.getType(pos);
            TokenizerUtil.debug(pos, context);
            while (Tokenizer2.COMMA == type) {
                TokenizerUtil.debug(() -> ",");
                ++pos;
                TokenizerUtil.debug(pos, context);
                pos = pickExpr(pos, arrayCount, context, hashArray, sql);
                type = hashArray.getType(pos);
            }
            type = hashArray.getType(pos);
            TokenizerUtil.debug(pos, context);
            if (type == Tokenizer2.RIGHT_PARENTHESES) {
                TokenizerUtil.debug(() -> ")");
                ++pos;
                return pos;
            }else {
                TokenizerUtil.debug(()->"pickFunctionCall :语法错误");
            }
        }
        return pos;//语法错误
    }
}

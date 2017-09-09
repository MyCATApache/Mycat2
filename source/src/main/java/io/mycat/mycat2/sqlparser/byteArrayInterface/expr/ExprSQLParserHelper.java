package io.mycat.mycat2.sqlparser.byteArrayInterface.expr;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.IntTokenHash;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.TokenHash;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;

import io.mycat.mycat2.sqlparser.byteArrayInterface.Tokenizer2;
import io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil;

/**
 * Created by jamie on 2017/9/5.
 */
public class ExprSQLParserHelper {
    /*
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
    public static boolean isComparisonOperatorByType(int c) {
        switch (c) {
            case Tokenizer2.EQUAL:
            case Tokenizer2.GREATER:
            case Tokenizer2.LESS:
            case Tokenizer2.COLON://!
                return true;
            default:
                return false;
        }
    }

    /**
     * comparison_operator: =
     * | >=
     * | >
     * | <=
     * | <
     * | <>
     * | !=
     */
    public static int pickComparisonOperator(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        int next;
        switch (hashArray.getType(pos++)) {
            case Tokenizer2.EQUAL:// =
                TokenizerUtil.debug(()->"comparisonOperator:=");
                return pos;
            case Tokenizer2.GREATER: {// >
                next = hashArray.getType(pos);
                if (next == Tokenizer2.EQUAL) { // =
                    TokenizerUtil.debug(()->"comparisonOperator:>=");
                    pos++;
                } else {
                    TokenizerUtil.debug(()->"comparisonOperator:>");
                }
                return pos;
            }
            case Tokenizer2.LESS: {// <
                next = hashArray.getType(pos);
                if (next == Tokenizer2.EQUAL) { // =
                    TokenizerUtil.debug(()->"comparisonOperator:<=");
                    pos++;
                } else if (next == Tokenizer2.GREATER) {// >
                    TokenizerUtil.debug(()->"comparisonOperator:<>");
                    pos++;
                } else {
                    TokenizerUtil.debug(()->"comparisonOperator:<");
                }
                return pos;
            }
            case Tokenizer2.COLON: {//!
                next = hashArray.getType(pos);
                if (next == Tokenizer2.EQUAL) { // =
                    TokenizerUtil.debug(()->"comparisonOperator:!=");
                    pos++;
                    return pos;
                }
            }
            default: {
                //语法错误
                break;
            }
        }
        return pos;
    }


    public static boolean isTimeUnit(long value) {
        if (TokenHash.MICROSECOND == value) {

        } else if (TokenHash.SECOND == value) {

        } else if (TokenHash.MINUTE == value) {

        } else if (TokenHash.HOUR == value) {

        } else if (TokenHash.MONTH == value) {

        } else if (TokenHash.DAY == value) {

        } else if (TokenHash.WEEK == value) {

        } else if (TokenHash.QUARTER == value) {

        } else if (TokenHash.YEAR == value) {

        } else if (TokenHash.SECOND_MICROSECOND == value) {

        } else if (TokenHash.MINUTE_MICROSECOND == value) {

        } else if (TokenHash.MINUTE_SECOND == value) {

        } else if (TokenHash.HOUR_MICROSECOND == value) {

        } else if (TokenHash.HOUR_SECOND == value) {

        } else if (TokenHash.HOUR_MINUTE == value) {

        } else if (TokenHash.DAY_MICROSECOND == value) {

        } else if (TokenHash.DAY_SECOND == value) {

        } else if (TokenHash.DAY_MINUTE == value) {

        } else if (TokenHash.DAY_HOUR == value) {

        } else if (TokenHash.YEAR_MONTH == value) {

        } else return false;

        return true;
    }

    /**
     * todo 捕获  TimeUnit 暂定返回 TimeUnit的类型
     *
     * @param value
     * @return
     */
    public static long pickTimeUnit(long value) {
        if (TokenHash.MICROSECOND == value) {
            return value;
        } else if (TokenHash.SECOND == value) {
            return value;
        } else if (TokenHash.MINUTE == value) {
            return value;
        } else if (TokenHash.HOUR == value) {
            return value;
        } else if (TokenHash.MONTH == value) {
            return value;
        } else if (TokenHash.DAY == value) {
            return value;
        } else if (TokenHash.WEEK == value) {
            return value;
        } else if (TokenHash.QUARTER == value) {
            return value;
        } else if (TokenHash.YEAR == value) {
            return value;
        } else if (TokenHash.SECOND_MICROSECOND == value) {
            return value;
        } else if (TokenHash.MINUTE_MICROSECOND == value) {
            return value;
        } else if (TokenHash.MINUTE_SECOND == value) {
            return value;
        } else if (TokenHash.HOUR_MICROSECOND == value) {
            return value;
        } else if (TokenHash.HOUR_SECOND == value) {
            return value;
        } else if (TokenHash.HOUR_MINUTE == value) {
            return value;
        } else if (TokenHash.DAY_MICROSECOND == value) {
            return value;
        } else if (TokenHash.DAY_SECOND == value) {
            return value;
        } else if (TokenHash.DAY_MINUTE == value) {
            return value;
        } else if (TokenHash.DAY_HOUR == value) {
            return value;
        } else if (TokenHash.YEAR_MONTH == value) {
            return value;
        } else return -1;
    }

    /**
     * A subquery's outer statement can be any one of: SELECT, INSERT, UPDATE, DELETE, SET, or DO.
     */
    public static boolean isSubquery(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        int intHash = hashArray.getIntHash(pos);
        switch (intHash) {
            case IntTokenHash.SELECT:
            case IntTokenHash.INSERT:
            case IntTokenHash.UPDATE:
            case IntTokenHash.DELETE:
            case IntTokenHash.SET:
                return true;
            default: {
                //todo 性能优化
                long longHash = hashArray.getHash(pos);
                if (longHash == TokenHash.DO) {
                    return true;
                }
                return false;
            }
        }
    }

    /**
     * todo 此函数没有实现
     *
     * @param pos
     * @param arrayCount
     * @param context
     * @param hashArray
     * @param sql
     * @return
     */
    public static int pickSubquery(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        int intHash = hashArray.getIntHash(pos);
        switch (intHash) {
            case IntTokenHash.SELECT:
            case IntTokenHash.INSERT:
            case IntTokenHash.UPDATE:
            case IntTokenHash.DELETE:
            case IntTokenHash.SET:
                //todo 性能优化
                long longHash = hashArray.getHash(pos);
                if (longHash == TokenHash.DO) {
                    return pos;
                }
                return pos;
        }
        return pos;
    }

    /**
     * search_modifier:
     * {
     * IN NATURAL LANGUAGE MODE
     * | IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION
     * | IN BOOLEAN MODE
     * | WITH QUERY EXPANSION
     * }
     */
    public static int pickSearchModifier(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.IN == longHash) {
             longHash = hashArray.getHash(++pos);
            if (TokenHash.BOOLEAN == longHash) {
                pos+=2;
                return pos;
            }else {
                pos+=3;
                if (pos< arrayCount) {
                    longHash = hashArray.getHash(pos);
                    if (TokenHash.WITH== longHash) {
                        //todo 捕获 N NATURAL LANGUAGE MODE WITH QUERY EXPANSION
                        pos+=3;
                        return pos;
                    }else {
                        return pos;
                    }
                }
            }
        } else if (TokenHash.WITH == longHash) {
            pos += 3;
            //todo 完整匹配
            return pos;
        }
        return pos;
    }
}

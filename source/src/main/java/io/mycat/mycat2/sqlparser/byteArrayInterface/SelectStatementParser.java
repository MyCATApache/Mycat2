package io.mycat.mycat2.sqlparser.byteArrayInterface;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.TokenHash;
import io.mycat.mycat2.sqlparser.byteArrayInterface.expr.ExprSQLParser;

/**
 * Created by jamie on 2017/9/8.
 */
public class SelectStatementParser {
    /**
     * 前置条件,传进来的pos指向select后面的token
     */
    public static int pickSelectStatement(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.SELECT==longHash){
            //测试的路径
            ++pos;
        }
        if (TokenHash.ALL == longHash) {
            TokenizerUtil.debug(() -> "ALL");
            ++pos;
        } else if (TokenHash.DISTINCT == longHash) {
            TokenizerUtil.debug(() -> "DISTINCT");
            ++pos;
        } else if (TokenHash.DISTINCTROW == longHash) {
            TokenizerUtil.debug(() -> "DISTINCTROW");
            ++pos;
        }
        if (TokenHash.HIGH_PRIORITY == longHash) {
            TokenizerUtil.debug(() -> "HIGH_PRIORITY");
            ++pos;
        }
        if (TokenHash.STRAIGHT_JOIN == longHash) {
            TokenizerUtil.debug(() -> "STRAIGHT_JOIN");
            ++pos;
        }
        if (TokenHash.SQL_SMALL_RESULT == longHash) {
            TokenizerUtil.debug(() -> "SQL_SMALL_RESULT");
            ++pos;
        }
        if (TokenHash.SQL_BIG_RESULT == longHash) {
            TokenizerUtil.debug(() -> "SQL_BIG_RESULT");
            ++pos;
        }
        if (TokenHash.SQL_BUFFER_RESULT == longHash) {
            TokenizerUtil.debug(() -> "SQL_BUFFER_RESULT");
            ++pos;
        }
        if (TokenHash.SQL_CACHE == longHash) {
            TokenizerUtil.debug(() -> "SQL_CACHE");
            ++pos;
        } else if (TokenHash.SQL_NO_CACHE == longHash) {
            TokenizerUtil.debug(() -> "SQL_NO_CACHE");
            ++pos;
        }
        if (TokenHash.SQL_CALC_FOUND_ROWS == longHash) {
            TokenizerUtil.debug(() -> "SQL_CALC_FOUND_ROWS");
            ++pos;
        }
        ///
        pos = pickSelectExpr(pos, arrayCount, context, hashArray, sql);
        int type = hashArray.getType(pos);
        while (Tokenizer2.COMMA == type) {
            ++pos;
            pos = pickSelectExpr(pos, arrayCount, context, hashArray, sql);
            type = hashArray.getType(pos);
        }
        longHash = hashArray.getHash(pos);
        if (TokenHash.FROM == longHash) {
            ++pos;
        }
        if (TokenHash.PARTITION == longHash) {
            ++pos;
        }
        if (TokenHash.WHERE == longHash) {
            ++pos;
            ExprSQLParser.pickExpr(pos, arrayCount, context, hashArray, sql);
        }
        if (TokenHash.HAVING == longHash) {
            ++pos;
        }
        if (TokenHash.ORDER == longHash) {
            ++pos;
        }
        if (TokenHash.LIMIT == longHash) {
            ++pos;
        }
        if (TokenHash.INTO == longHash) {
            ++pos;
        }
        if (TokenHash.FOR == longHash) {
            ++pos;
        }
        return pos;
    }

    public static int pickSelectExpr(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        TokenizerUtil.debug(pos, context);
        ++pos;
        longHash = hashArray.getHash(pos);
        TokenizerUtil.debug(pos, context);
        ++pos;
        longHash = hashArray.getHash(pos);
        TokenizerUtil.debug(pos, context);
        ++pos;
        return pos;
    }

}

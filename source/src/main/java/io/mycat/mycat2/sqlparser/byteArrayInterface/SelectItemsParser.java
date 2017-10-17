package io.mycat.mycat2.sqlparser.byteArrayInterface;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.FunctionHash;
import io.mycat.mycat2.sqlparser.IntTokenHash;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.TokenHash;
import sun.tools.jstat.Token;

import static io.mycat.mycat2.sqlparser.IntTokenHash.SQL_DELIMETER;

public class SelectItemsParser {

    private static boolean isFunction(int pos, HashArray hashArray) {
        return hashArray.getType(pos) == Tokenizer2.LEFT_PARENTHESES;
    }

    private static boolean isListFinish(int intHash, long hash) {
        switch (intHash) {
            case IntTokenHash.SQL_DELIMETER:
                return true;
            case IntTokenHash.FROM:
                if (hash == TokenHash.FROM)
                    return true;
                break;
            case IntTokenHash.ORDER:
                if (hash == TokenHash.ORDER)
                    return true;
                break;
            case IntTokenHash.INTO:
                if (hash == TokenHash.INTO)
                    return true;
                break;
            default:
                return false;
        }
        return false;
    }

    public static int pickItemList(int pos, final int arrayCount, HashArray hashArray, BufferSQLContext context) {
        int intHash;
        int size;
        while (pos < arrayCount) {
            intHash = hashArray.getIntHash(pos);
            size = intHash & 0xFFFF;

            switch (size) {
                case 4:
                    if (intHash == IntTokenHash.USER && hashArray.getHash(pos) == TokenHash.USER && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.USER);
                        pos+=2;
                    } else if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
                    break;
                case 6:
                    if (intHash == IntTokenHash.SCHEMA && hashArray.getHash(pos) == TokenHash.SCHEMA && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.SCHEMA);
                        pos+=2;
                    } else if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
                    break;
                case 7:
                    if (intHash == IntTokenHash.CHARSET && hashArray.getHash(pos) == TokenHash.CHARSET && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.CHARSET);
                        pos+=2;
                    } else if (intHash == IntTokenHash.VERSION && hashArray.getHash(pos) == TokenHash.VERSION && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.VERSION);
                        pos+=2;
                    } else if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
                    break;
                case 8:
                    if (intHash == IntTokenHash.DATABASE && hashArray.getHash(pos) == TokenHash.DATABASE && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.DATABASE);
                        pos+=2;
                    } else if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
                    break;
                case 9:
                    if (intHash == IntTokenHash.BENCHMARK && hashArray.getHash(pos) == TokenHash.BENCHMARK && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.BENCHMARK);
                        pos+=2;
                    } else if (intHash == IntTokenHash.COLLATION && hashArray.getHash(pos) == TokenHash.COLLATION && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.COLLATION);
                        pos+=2;
                    } else if (intHash == IntTokenHash.ROW_COUNT && hashArray.getHash(pos) == TokenHash.ROW_COUNT && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.ROW_COUNT);
                        pos+=2;
                    } else if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
                    break;
                case 10:
                    if (intHash == IntTokenHash.FOUND_ROWS && hashArray.getHash(pos) == TokenHash.FOUND_ROWS && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.FOUND_ROWS);
                        pos+=2;
                    } else if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
                    break;
                case 11:
                    if (intHash == IntTokenHash.SYSTEM_USER && hashArray.getHash(pos) == TokenHash.SYSTEM_USER && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.SYSTEM_USER);
                        pos+=2;
                    } else if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
                    break;
                case 12:
                    if (intHash == IntTokenHash.COERCIBILITY && hashArray.getHash(pos) == TokenHash.COERCIBILITY && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.COERCIBILITY);
                        pos+=2;
                    } else if (intHash == IntTokenHash.CURRENT_USER && hashArray.getHash(pos) == TokenHash.CURRENT_USER && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.CURRENT_USER);
                        pos+=2;
                    } else if (intHash == IntTokenHash.SESSION_USER && hashArray.getHash(pos) == TokenHash.SESSION_USER && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.SESSION_USER);
                        pos+=2;
                    } else if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
                    break;
                case 13:
                    if (intHash == IntTokenHash.CONNECTION_ID && hashArray.getHash(pos) == TokenHash.CONNECTION_ID && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.CONNECTION_ID);
                        pos+=2;
                    } else if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
                    break;
                case 14:
                    if (intHash == IntTokenHash.LAST_INSERT_ID && hashArray.getHash(pos) == TokenHash.LAST_INSERT_ID && isFunction(++pos, hashArray)) {
                        context.setSelectItem(FunctionHash.LAST_INSERT_ID);
                        pos+=2;
                    } else if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
                    break;
                default:
                    if (isListFinish(intHash, hashArray.getHash(pos))) {
                        return pos;
                    } else {
                        pos++;
                    }
            }
        }
        return pos;
    }
}

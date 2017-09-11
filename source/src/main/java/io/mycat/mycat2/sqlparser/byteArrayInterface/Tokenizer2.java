package io.mycat.mycat2.sqlparser.byteArrayInterface;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;

import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

/**
 * Created by Kaiz on 2017/3/21.
 */
public class Tokenizer2 {

    public static final int DIGITS = 1;
    public static final int CHARS = 2;
    public static final int STRINGS = 3;
    public static final int MINUS = 4;
    public static final int SHARP = 5;
    public static final int DIVISION = 6;
    public static final byte AT = 7;
    public static final byte COMMA = 8;
    public static final byte BACK_SLASH = 9;
    public static final byte LEFT_PARENTHESES = 10;
    public static final byte RIGHT_PARENTHESES = 11;
    public static final byte SEMICOLON = 12;
    public static final byte STAR = 13;
    public static final byte EQUAL = 14;
    public static final byte PLUS = 15;
    public static final byte LESS = 16;
    public static final byte GREATER = 17;
    public static final byte DOT = 18;
    public static final byte ANNOTATION_BALANCE = 20;
    public static final byte ANNOTATION_START = 21;
    public static final byte ANNOTATION_END = 22;
    public static final byte COLON = 23;
    public static final byte TOBER =24;//~
    public static final byte QUESTION_MARK =25;//?
    public static final byte OR =26;
    public static final byte LEFT_CURLY_BRACKET =27;//{
    public static final byte RIGHT_CURLY_BRACKET =28;//}
    public static final byte AND =29;
    public static final byte PERCENT =30;
    public static final byte CARET =31;
    ByteArrayInterface sql;
    final byte[] charType = new byte[512];
    HashArray hashArray;



    public Tokenizer2(HashArray hashArray) {
        this.hashArray = hashArray;
        //// TODO: 2017/2/21 可能需要调整顺序进行优化
        IntStream.rangeClosed('0', '9').forEach(c -> charType[c<<1] = DIGITS);
        IntStream.rangeClosed('A', 'Z').forEach(c -> charType[c<<1] = CHARS);
        IntStream.rangeClosed('a', 'z').forEach(c -> charType[c<<1] = CHARS);
        charType['_'<<1] = CHARS;
        charType['$'<<1] = CHARS;
        charType['.'<<1] = DOT;
        charType[','<<1] = COMMA;
        //字符串
        charType['"'<<1] = STRINGS;
        charType['\''<<1] = STRINGS;
        charType['\\'<<1] = BACK_SLASH;
        //sql分隔
        charType['('<<1] = LEFT_PARENTHESES;
        charType[')'<<1] = RIGHT_PARENTHESES;
        charType[';'<<1] = SEMICOLON;
        //（可能的）注释和运算符
        charType['-'<<1] = MINUS;
        charType['/'<<1] = DIVISION;
        charType['#'<<1] = SHARP;
        charType['*'<<1] = STAR;
        charType['='<<1] = EQUAL;
        charType['+'<<1] = PLUS;
        charType['<'<<1] = LESS;
        charType['>'<<1] = GREATER;
        charType['@'<<1] = AT;
        charType['!'<<1] = COLON;
        charType['~'<<1] = TOBER;
        charType['?'<<1] = QUESTION_MARK;
        charType['|'<<1] = OR;
        charType['}'<<1] = RIGHT_CURLY_BRACKET;
        charType['{'<<1] = LEFT_CURLY_BRACKET;
        charType['%'<<1] = PERCENT;
        charType['^'<<1] = CARET;
        charType['&'<<1] =AND;
        charType[('$'<<1)+1] = 1;
        IntStream.rangeClosed('0', '9').forEach(c -> charType[(c<<1)+1] = (byte)(c-'0'+2));
        IntStream.rangeClosed('A', 'Z').forEach(c -> charType[(c<<1)+1] = (byte)(c-'A'+12));
        IntStream.rangeClosed('a', 'z').forEach(c -> charType[(c<<1)+1] = (byte)(c-'a'+12));
        charType[('_'<<1)+1] = 38;

    }

    int parseToken(ByteArrayInterface sql, int pos, final int sqlLength, byte c) {
        int cType;
        int start = pos;
        int size = 1;
        long hash = c = charType[(c<<1)+1];
        int type = 1315423911;
        type ^= (type<<5) + c + (type>>2);
        while (++pos < sqlLength && (((cType = charType[(c = sql.get(pos))<<1]) == 2) || cType == 1) ) {
            cType = charType[(c<<1)+1];
            hash = (hash*41)+cType;//别问我为什么是41
            type ^= (type<<5) + cType + (type>>2);
            size++;
        }
        hashArray.set(type, start, size, hash);
        return pos;
    }

    int parseString(ByteArrayInterface sql, int pos, final int sqlLength, int startSign) {
        int size = 1;
        int start = pos;
        int c;
        while (++pos < sqlLength ) {
            c = sql.get(pos);
            if (c == '\\') {
                pos+=2;
            } else if (c == startSign ) {
                size++;
                break;
            } else {
                size++;
            }
        }
        hashArray.set(STRINGS, start, size, 0L);
        return ++pos;
    }

    int parseDigits(ByteArrayInterface sql, int pos, final int sqlLength) {
        int start = pos;
        int size = 1;
        while (++pos<sqlLength && charType[sql.get(pos)<<1] == DIGITS) {
            size++;
        }
        hashArray.set(DIGITS, start, size);
        return pos;
    }

    int parseAnnotation(ByteArrayInterface sql, int pos, final int sqlLength) {
        hashArray.set(ANNOTATION_START, pos-2, 2);
        //byte cur = sql[pos];
        //byte next = sql[++pos];
        byte c;
        byte cType;
        while (pos < sqlLength) {
            c = sql.get(pos);
            cType = charType[c<<1];
            switch (cType) {
                case DIGITS:
                    pos = parseDigits(sql, pos, sqlLength);
                    break;
                case CHARS:
                    pos = parseToken(sql, pos, sqlLength, c);
                    break;
                case STAR:
                    if (sql.get(++pos) == '/') {
                        hashArray.set(ANNOTATION_END, pos-1, 2);
                        return ++pos;
                    } else {
                        hashArray.set(cType, pos-1, 1);
                    }
                    break;
                case EQUAL:
                    hashArray.set(cType,pos,1);
                default:
                    pos++;
                    break;
            }
        }
        return pos;
    }

    int skipSingleLineComment(ByteArrayInterface sql, int pos, final int sqlLength) {
        while (++pos < sqlLength && sql.get(pos)!='\n');
        return pos;
    }

    int skipMultiLineComment(ByteArrayInterface sql, int pos, final int sqlLength, byte pre) {
        //int start = pos-2;
        //int size=2;
        byte cur = sql.get(pos);
        while (pos < sqlLength) {
            if (pre == '*' && cur == '/' ) {
                return ++pos;
            }
            pos++;
            pre = cur;
            cur = sql.get(pos);
        }
        //hashArray.set(COMMENTS, start, size);
        return pos;
    }

    public void tokenize(ByteArrayInterface sql) {
        int pos = sql.getOffset();
        final int sqlLength = sql.length();
        this.sql = sql;
        hashArray.init();
        byte c;
        byte cType;
        byte next;
        while (pos < sqlLength) {
            c = sql.get(pos);
            cType = charType[c<<1];

            switch (cType) {
                case 0:
                    pos++;
                    break;
                case CHARS:
                    pos = parseToken(sql, pos, sqlLength, c);
                    break;
                case DIGITS:
                    pos = parseDigits(sql, pos, sqlLength);
                    break;
                case STRINGS:
                    pos = parseString(sql, pos, sqlLength, c);
                    break;
                case MINUS:
                    if (sql.get(++pos)!='-') {
                        hashArray.set(MINUS, pos-1, 1);
                    } else {
                        pos = skipSingleLineComment(sql, pos, sqlLength);
                    }
                    break;
                case SHARP:
                    pos = skipSingleLineComment(sql, pos, sqlLength);
                    break;
                case DIVISION:
                    next = sql.get(++pos);
                    if (next == '*') {
                        next = sql.get(++pos);
                        if (next == ' ') {
                            //处理新版mycat注解
                            if ((sql.get(++pos)&0xDF) == 'M' && (sql.get(++pos)&0xDF) == 'Y' &&(sql.get(++pos)&0xDF) == 'C' &&(sql.get(++pos)&0xDF) == 'A' &&(sql.get(++pos)&0xDF) == 'T'
                                    && sql.get(++pos) == ':') {
                                pos = parseAnnotation(sql, pos, sqlLength);
                            } else {
                                pos = skipMultiLineComment(sql, ++pos, sqlLength, next);
                            }
                        }
//                        else if ((next&0xDF) == 'B' && (sql.get(++pos)&0xDF) == 'A' && (sql.get(++pos)&0xDF) == 'L' && (sql.get(++pos)&0xDF) == 'A'
//                                && (sql.get(++pos)&0xDF) == 'N' && (sql.get(++pos)&0xDF) == 'C' && (sql.get(++pos)&0xDF) == 'E'
//                                && sql.get(++pos) == '*' && sql.get(++pos) == '/' ) { //还有 /*balance*/ 注解
//                            hashArray.set(ANNOTATION_BALANCE, pos-1, 1);
//                            hashArray.set(ANNOTATION_END, pos, 1);
//                            pos++;
//                        }
                        else
                            pos = skipMultiLineComment(sql, ++pos, sqlLength, next);
                    } else if (next == '/') {
                        pos = skipSingleLineComment(sql, pos, sqlLength);
                    } else {
                        hashArray.set(cType, pos++, 1);
                    }
                    break;
                case AT:
                    next = sql.get(++pos);
                    if (next == '@') {
                        //parse system infomation
                    }
                default:
                    hashArray.set(cType, pos++, 1);
            }
        }
    }

    public static void main(String[] args) {
      BufferSQLParser sqlParser2=new BufferSQLParser();
      BufferSQLContext context2=new BufferSQLContext();
      sqlParser2.parse("AUTOCOMMIT".getBytes(StandardCharsets.UTF_8),context2);
    }
}

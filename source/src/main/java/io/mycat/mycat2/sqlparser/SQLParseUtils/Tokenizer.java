package io.mycat.mycat2.sqlparser.SQLParseUtils;

import java.util.stream.IntStream;

/**
 * Created by Kaiz on 2017/3/21.
 */
public class Tokenizer {

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

    byte[] sql;
    final byte[] charType = new byte[512];
    HashArray hashArray;



    public Tokenizer(HashArray hashArray) {
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

        charType[('$'<<1)+1] = 1;
        IntStream.rangeClosed('0', '9').forEach(c -> charType[(c<<1)+1] = (byte)(c-'0'+2));
        IntStream.rangeClosed('A', 'Z').forEach(c -> charType[(c<<1)+1] = (byte)(c-'A'+12));
        IntStream.rangeClosed('a', 'z').forEach(c -> charType[(c<<1)+1] = (byte)(c-'a'+12));
        charType[('_'<<1)+1] = 38;

    }

    int parseToken(byte[] sql, int pos, final int sqlLength, byte c) {
        int cType;
        int start = pos;
        int size = 1;
        long hash = c = charType[(c<<1)+1];
        int type = 1315423911;
        type ^= (type<<5) + c + (type>>2);
        while (++pos < sqlLength && (((cType = charType[(c = sql[pos])<<1]) == 2) || cType == 1) ) {
            cType = charType[(c<<1)+1];
            hash = (hash*41)+cType;//别问我为什么是41
            type ^= (type<<5) + cType + (type>>2);
            size++;
        }
        hashArray.set(type, start, size, hash);
        return pos;
    }

    int parseString(byte[] sql, int pos, final int sqlLength, int startSign) {
        int size = 1;
        int start = pos;
        int c;
        while (++pos < sqlLength ) {
            c = sql[pos];
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

    int parseDigits(byte[] sql, int pos, final int sqlLength) {
        int start = pos;
        int size = 1;
        while (++pos<sqlLength && charType[sql[pos]<<1] == DIGITS) {
            size++;
        }
        hashArray.set(DIGITS, start, size);
        return pos;
    }

    int parseAnnotation(byte[] sql, int pos, final int sqlLength) {
        hashArray.set(ANNOTATION_START, pos-2, 2);
        //byte cur = sql[pos];
        //byte next = sql[++pos];
        byte c;
        byte cType;
        while (pos < sqlLength) {
            c = sql[pos];
            cType = charType[c<<1];
            switch (cType) {
                case DIGITS:
                    pos = parseDigits(sql, pos, sqlLength);
                    break;
                case CHARS:
                    pos = parseToken(sql, pos, sqlLength, c);
                    break;
                case STAR:
                    if (sql[++pos] == '/') {
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

    int skipSingleLineComment(byte[] sql, int pos, final int sqlLength) {
        while (++pos < sqlLength && sql[pos]!='\n');
        return pos;
    }

    int skipMultiLineComment(byte[] sql, int pos, final int sqlLength, byte pre) {
        //int start = pos-2;
        //int size=2;
        byte cur = sql[pos];
        while (pos < sqlLength) {
            if (pre == '*' && cur == '/' ) {
                return ++pos;
            }
            pos++;
            pre = cur;
            cur = sql[pos];
        }
        //hashArray.set(COMMENTS, start, size);
        return pos;
    }

    public void tokenize(byte[] sql) {
        int pos = 0;
        final int sqlLength = sql.length;
        this.sql = sql;
        hashArray.init();
        byte c;
        byte cType;
        byte next;
        while (pos < sqlLength) {
            c = sql[pos];
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
                    if (sql[++pos]!='-') {
                        hashArray.set(MINUS, pos-1, 1);
                    } else {
                        pos = skipSingleLineComment(sql, pos, sqlLength);
                    }
                    break;
                case SHARP:
                    pos = skipSingleLineComment(sql, pos, sqlLength);
                    break;
                case DIVISION:
                    next = sql[++pos];
                    if (next == '*') {
                        next = sql[++pos];
                        if (next == '!'||next == '#') {
                            //处理mycat注解
                            if ((sql[++pos]&0xDF) == 'M' && (sql[++pos]&0xDF) == 'Y' &&(sql[++pos]&0xDF) == 'C' &&(sql[++pos]&0xDF) == 'A' &&(sql[++pos]&0xDF) == 'T'
                                    && sql[++pos] == ':') {
                                pos = parseAnnotation(sql, pos, sqlLength);
                            } else {
                                pos = skipMultiLineComment(sql, ++pos, sqlLength, next);
                            }
                        } else if ((next&0xDF) == 'B' && (sql[++pos]&0xDF) == 'A' && (sql[++pos]&0xDF) == 'L' && (sql[++pos]&0xDF) == 'A'
                                && (sql[++pos]&0xDF) == 'N' && (sql[++pos]&0xDF) == 'C' && (sql[++pos]&0xDF) == 'E'
                                && sql[++pos] == '*' && sql[++pos] == '/' ) { //还有 /*balance*/ 注解
                            hashArray.set(ANNOTATION_BALANCE, pos-1, 1);
                            hashArray.set(ANNOTATION_END, pos, 1);
                            pos++;
                        } else
                            pos = skipMultiLineComment(sql, ++pos, sqlLength, next);
                    } else if (next == '/') {
                        pos = skipSingleLineComment(sql, pos, sqlLength);
                    } else {
                        hashArray.set(charType[next<<1], pos++, 1);
                    }
                    break;
                case AT:
                    next = sql[++pos];
                    if (next == '@') {
                        //parse system infomation
                    }
                default:
                    hashArray.set(cType, pos++, 1);
            }
        }
    }

}

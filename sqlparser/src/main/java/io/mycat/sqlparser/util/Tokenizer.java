package io.mycat.sqlparser.util;


import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

/**
 * Created by Kaiz on 2017/3/21.
 */
public class Tokenizer {

  public static final int DIGITS = 1;// [0-9]
  public static final int CHARS = 2;//   charType['_'<<1] = CHARS,charType['$'<<1] = CHARS;
  public static final int STRINGS = 3;// "
  public static final int MINUS = 4;// -
  public static final int SHARP = 5;// #
  public static final int DIVISION = 6;// /
  public static final byte AT = 7;// @
  public static final byte COMMA = 8;// ,
  public static final byte BACK_SLASH = 9;// \ 反斜杠
  public static final byte LEFT_PARENTHESES = 10;// (
  public static final byte RIGHT_PARENTHESES = 11;// )
  public static final byte SEMICOLON = 12;// ;
  public static final byte STAR = 13;// *
  public static final byte EQUAL = 14;// =
  public static final byte PLUS = 15;// +
  public static final byte LESS = 16;// <
  public static final byte GREATER = 17;// >
  public static final byte DOT = 18;// .
  public static final byte ANNOTATION_BALANCE = 20;
  public static final byte ANNOTATION_START = 21;
  public static final byte ANNOTATION_END = 22;
  public static final byte COLON = 23;// !
  public static final byte TOBER = 24;//~
  public static final byte QUESTION_MARK = 25;//?
  public static final byte OR = 26;// |
  public static final byte LEFT_CURLY_BRACKET = 27;//{
  public static final byte RIGHT_CURLY_BRACKET = 28;//}
  public static final byte AND = 29;// &
  public static final byte PERCENT = 30;//%
  public static final byte CARET = 31;//^

  public static final byte LESS_EQUAL_GREATER = 32;// <=>
  public static final byte LESS_EQUAL = 33;// <=
  public static final byte GREATER_EQUAL = 34;// >=
  public static final byte LESS_GREATER = 35;// <>
  public static final byte COLON_EQUAL = 36;// !=

  public static final byte OR_OR = 37;// ||
  public static final byte AND_AND = 38;// &&
  public static final byte LESS_LESS = 39;// <<
  public static final byte GREATER_GREATER = 40;// >>
  final byte[] charType = new byte[512];
  ByteArrayView sql;
  HashArray hashArray;


  public Tokenizer() {
    //// TODO: 2017/2/21 可能需要调整顺序进行优化
    IntStream.rangeClosed('0', '9').forEach(c -> charType[c << 1] = DIGITS);
    IntStream.rangeClosed('A', 'Z').forEach(c -> charType[c << 1] = CHARS);
    IntStream.rangeClosed('a', 'z').forEach(c -> charType[c << 1] = CHARS);
    charType['_' << 1] = CHARS;
    charType['$' << 1] = CHARS;
    charType['.' << 1] = DOT;
    charType[',' << 1] = COMMA;
    //字符串
    charType['"' << 1] = STRINGS;
    charType['\'' << 1] = STRINGS;
    charType['\\' << 1] = BACK_SLASH;
    //sql分隔
    charType['(' << 1] = LEFT_PARENTHESES;
    charType[')' << 1] = RIGHT_PARENTHESES;
    charType[';' << 1] = SEMICOLON;
    //（可能的）注释和运算符
    charType['-' << 1] = MINUS;
    charType['/' << 1] = DIVISION;
    charType['#' << 1] = SHARP;
    charType['*' << 1] = STAR;
    charType['=' << 1] = EQUAL;
    charType['+' << 1] = PLUS;
    charType['<' << 1] = LESS;
    charType['>' << 1] = GREATER;
    charType['@' << 1] = AT;
    charType['!' << 1] = COLON;
    charType['~' << 1] = TOBER;
    charType['?' << 1] = QUESTION_MARK;
    charType['|' << 1] = OR;
    charType['}' << 1] = RIGHT_CURLY_BRACKET;
    charType['{' << 1] = LEFT_CURLY_BRACKET;
    charType['%' << 1] = PERCENT;
    charType['^' << 1] = CARET;
    charType['&' << 1] = AND;
    charType[('$' << 1) + 1] = 1;
    IntStream.rangeClosed('0', '9').forEach(c -> charType[(c << 1) + 1] = (byte) (c - '0' + 2));
    IntStream.rangeClosed('A', 'Z').forEach(c -> charType[(c << 1) + 1] = (byte) (c - 'A' + 12));
    IntStream.rangeClosed('a', 'z').forEach(c -> charType[(c << 1) + 1] = (byte) (c - 'a' + 12));
    charType[('_' << 1) + 1] = 38;

  }

  public static void main(String[] args) {
    BufferSQLParser sqlParser2 = new BufferSQLParser();
    BufferSQLContext context2 = new BufferSQLContext();
    sqlParser2.parse("AUTOCOMMIT".getBytes(StandardCharsets.UTF_8), context2);
  }

  int parseToken(ByteArrayView sql, int pos, final int sqlLength, byte c) {
    int cType;
    int start = pos;
    int size = 1;
    long hash = c = charType[(c << 1) + 1];
    int type = 1315423911;
    type ^= (type << 5) + c + (type >> 2);
    while (++pos < sqlLength && (((cType = charType[(c = sql.getByte(pos)) << 1]) == 2)
        || cType == 1)) {
      cType = charType[(c << 1) + 1];
      hash = (hash * 41) + cType;//别问我为什么是41
      type ^= (type << 5) + cType + (type >> 2);
      size++;
    }
    hashArray.set(type, start, size, hash);
    return pos;
  }

  int parseString(ByteArrayView sql, int pos, final int sqlLength, byte startSign) {
    int size = 1;
    int start = pos;
    long hash = 1315423911;
    byte c = 0;
    while (++pos < sqlLength) {
      c = sql.getByte(pos);
      if (c == '\\') {
        pos += 2;
      } else if (c == startSign) {
        size++;
        break;
      } else {
        hash ^= (hash << 5) + c + (hash >> 2);//使用JSHash对字符串进行哈希
        size++;
      }
    }
    hashArray.set(STRINGS, start, size, hash);
    return ++pos;
  }

  int parseDigits(ByteArrayView sql, int pos, final int sqlLength,
      byte c) {  // TODO: 需要增加小数和hex类型处理吗？
    int start = pos;
    int size = 1;
    long longValue = (long) (c - '0');
    while (++pos < sqlLength && charType[(c = sql.getByte(pos)) << 1] == DIGITS) {
      longValue = longValue * 10L + (long) (c - '0');
      size++;
    }
    hashArray.set(DIGITS, start, size, longValue);
    return pos;
  }

  int parseAnnotation(ByteArrayView sql, int flagPos, int flagLength, int pos,
      final int sqlLength) {
    hashArray.set(ANNOTATION_START, pos - 2, 2);
    hashArray.set(CHARS, flagPos, flagLength);
    //byte cur = sql[pos];
    //byte next = sql[++pos];
    byte c;
    byte cType;
    while (pos < sqlLength) {
      c = sql.getByte(pos);
      cType = charType[c << 1];
      switch (cType) {
        case DIGITS:
          pos = parseDigits(sql, pos, sqlLength, c);
          break;
        case CHARS:
          pos = parseToken(sql, pos, sqlLength, c);
          break;
        case STRINGS:
          pos = parseString(sql, pos, sqlLength, c);
          break;
        case STAR:
          if (sql.getByte(++pos) == '/') {
            hashArray.set(ANNOTATION_END, pos - 1, 2);
            return ++pos;
          } else {
            hashArray.set(cType, pos - 1, 1);
          }
          break;
        case COMMA:
          hashArray.set(cType, pos, 1);
          pos++;
          break;
        case EQUAL:
          hashArray.set(cType, pos, 1);
          pos++;
          break;
        default:
          pos++;
          break;
      }
    }
    return pos;
  }

  int skipSingleLineComment(ByteArrayView sql, int pos, final int sqlLength) {
    while (++pos < sqlLength && sql.getByte(pos) != '\n') {
    }
    return pos;
  }

  int skipMultiLineComment(ByteArrayView sql, int pos, final int sqlLength, byte pre) {
    //int start = pos-2;
    //int size=2;
    byte cur = sql.getByte(pos);
    while (pos < sqlLength) {
      if (pre == '*' && cur == '/') {
        return ++pos;
      }
      pos++;
      pre = cur;
      cur = sql.getByte(pos);
    }
    //hashArray.set(COMMENTS, start, size);
    return pos;
  }

  public void tokenize(ByteArrayView sql, HashArray hashArray) {
    int pos = sql.getOffset();
    final int sqlLength = sql.length() + pos;
    this.sql = sql;
    this.hashArray = hashArray;
    byte c;
    byte cType;
    byte next;
    while (pos < sqlLength) {
      c = sql.getByte(pos);
      cType = charType[c << 1];
      switch (cType) {
        case 0:
          pos++;
          break;
        case CHARS:
          pos = parseToken(sql, pos, sqlLength, c);
          break;
        case DIGITS:
          pos = parseDigits(sql, pos, sqlLength, c);
          break;
        case STRINGS:
          pos = parseString(sql, pos, sqlLength, c);
          break;
        case MINUS:
          if (sql.getByte(++pos) != '-') {
            hashArray.set(MINUS, pos - 1, 1);
          } else {
            pos = skipSingleLineComment(sql, pos, sqlLength);
          }
          break;
        case SHARP:
          pos = skipSingleLineComment(sql, pos, sqlLength);
          break;
        case DIVISION: {// /
          final int start = pos;
          next = sql.getByte(++pos);
          if (next == '*') {//  /*
            next = sql.getByte(++pos);
            //处理新版 mycat 注解
            if (next == ' ') {
              int flagStart = ++pos;
              if ((sql.getByte(flagStart) & 0xDF) == 'M' && (sql.getByte(++pos) & 0xDF) == 'Y'
                  && (sql.getByte(++pos) & 0xDF) == 'C' && (sql.getByte(++pos) & 0xDF) == 'A'
                  && (sql.getByte(++pos) & 0xDF) == 'T'
                  && sql.getByte(++pos) == ':') {
                pos = parseAnnotation(sql, flagStart, pos - flagStart, pos, sqlLength);
              } else if ((sql.getByte(flagStart) & 0xDF) == '_') {
                for (; (sql.getByte(pos)) != ':' && pos < sqlLength; pos++) {
                }
                if (sql.getByte(pos) == ':') {
                  pos = parseAnnotation(sql, flagStart, pos - flagStart, pos, sqlLength);
                } else {
                  pos = skipMultiLineComment(sql, ++pos, sqlLength, next);
                }
              } else {
                pos = skipMultiLineComment(sql, ++pos, sqlLength, next);
              }
            } else if ((next == '!' && (next = sql.getByte(++pos)) == ' ')) {
              int flagStart = ++pos;
              if ((sql.getByte(flagStart) & 0xDF) == 'M' && (sql.getByte(++pos) & 0xDF) == 'Y'
                  && (sql.getByte(++pos) & 0xDF) == 'C' && (sql.getByte(++pos) & 0xDF) == 'A'
                  && (sql.getByte(++pos) & 0xDF) == 'T'
                  && sql.getByte(++pos) == ':') {
                pos = parseAnnotation(sql, flagStart, pos - flagStart, pos, sqlLength);
                //  sql.set(tmppos, (byte)' '); @todo  ??????????
              } else if ((sql.getByte(flagStart) & 0xDF) == '_') {
                for (; (sql.getByte(pos)) != ':' && pos < sqlLength; pos++) {
                }
                if (sql.getByte(pos) == ':') {
                  pos = parseAnnotation(sql, flagStart, pos - flagStart, pos, sqlLength);
                } else {
                  pos = skipMultiLineComment(sql, ++pos, sqlLength, next);
                }
              } else {
                pos = skipMultiLineComment(sql, ++pos, sqlLength, next);
              }
            }
//                        else if ((next&0xDF) == 'B' && (sql.getByte(++pos)&0xDF) == 'A' && (sql.getByte(++pos)&0xDF) == 'L' && (sql.getByte(++pos)&0xDF) == 'A'
//                                && (sql.getByte(++pos)&0xDF) == 'N' && (sql.getByte(++pos)&0xDF) == 'C' && (sql.getByte(++pos)&0xDF) == 'E'
//                                && sql.getByte(++pos) == '*' && sql.getByte(++pos) == '/' ) { //还有 /*balance*/ 注解
//                            hashArray.set(ANNOTATION_BALANCE, pos-1, 1);
//                            hashArray.set(ANNOTATION_END, pos, 1);
//                            pos++;
//                        }
            else {
              pos = skipMultiLineComment(sql, ++pos, sqlLength, next);
            }
          } else if (next == '/') {// //
            pos = skipSingleLineComment(sql, pos, sqlLength);
          } else {
            hashArray.set(cType, start, 1);
          }
          break;
        }
        case LESS:// <
        {
          final int start = pos;
          next = sql.getByte(++pos);
          if (next == '=') {// <=
            next = sql.getByte(++pos);
            if (next == '>') {// <=>
              ++pos;
              hashArray.set(Tokenizer.LESS_EQUAL_GREATER, start, 3);
              break;
            } else {//<=
              hashArray.set(Tokenizer.LESS_EQUAL, start, 2);
              break;
            }
          } else if (next == '<') {// <<
            ++pos;
            hashArray.set(Tokenizer.LESS_LESS, start, 2);
            break;
          } else if (next == '>') {// <>
            ++pos;
            hashArray.set(Tokenizer.LESS_GREATER, start, 2);
            break;
          } else {
            hashArray.set(cType, start, 1);
          }
          break;
        }
        case GREATER:// >
        {
          final int start = pos;
          next = sql.getByte(++pos);
          if (next == '=') {// >=
            ++pos;
            hashArray.set(Tokenizer.GREATER_EQUAL, start, 2);
            break;
          } else if (next == '>') {// >>
            ++pos;
            hashArray.set(Tokenizer.GREATER_GREATER, start, 2);
            break;
          } else {
            hashArray.set(cType, start, 1);
          }
          break;
        }
        case OR: {
          final int start = pos;
          next = sql.getByte(++pos);
          if (next == '|') {
            ++pos;
            hashArray.set(OR_OR, start, 2);
          } else {
            hashArray.set(cType, start, 1);
          }
          break;
        }
        case AND: {
          int start = pos;
          next = sql.getByte(++pos);
          if (next == '&') {
            ++pos;
            hashArray.set(AND_AND, start, 2);
          } else {
            hashArray.set(cType, start, 1);
          }
          break;
        }
        case COLON: {
          final int start = pos;
          next = sql.getByte(++pos);
          if (next == '=') {
            hashArray.set(COLON_EQUAL, start, 2);
          } else {
            hashArray.set(cType, start, 1);
          }
          break;
        }
        case AT:
          next = sql.getByte(++pos);
          if (next == '@') {
            //parse system infomation
          }
        default:
          hashArray.set(cType, pos++, 1);
      }
    }
  }
}
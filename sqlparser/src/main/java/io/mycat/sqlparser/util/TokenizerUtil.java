package io.mycat.sqlparser.util;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.util.function.Supplier;

/**
 * Created by jamie on 2017/8/31.
 */
public class TokenizerUtil {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(TokenizerUtil.class);

  /**
   * 获取数字
   */
  public static long pickNumber(int pos, HashArray hashArray, ByteArrayView buffer) {
    int value = 0;
    int start = hashArray.getPos(pos);
    int end = start + hashArray.getSize(pos);
    for (int i = start; i < end; i++) {
      int l = buffer.getByte(i);
      int r = (l - '0');
      value = (value * 10) + (r);
    }
    return value;
  }

  /**
   * 获取bytes 数组
   */
  public static byte[] pickBytes(int pos, HashArray hashArray, ByteArrayView buffer) {
    int start = hashArray.getPos(pos);
    int length = hashArray.getSize(pos);
    return buffer.getBytes(start, length);
  }

  public static boolean isAlias(int pos, int type, HashArray hashArray) { //需要优化成数组判断
    switch (type) {
      case IntTokenHash.WHERE:
        return hashArray.getHash(pos) != TokenHash.WHERE;
      case IntTokenHash.GROUP:
        return hashArray.getHash(pos) != TokenHash.GROUP;
      case IntTokenHash.ORDER:
        return hashArray.getHash(pos) != TokenHash.ORDER;
      case IntTokenHash.LIMIT:
        return hashArray.getHash(pos) != TokenHash.LIMIT;
      case IntTokenHash.JOIN:
        return hashArray.getHash(pos) != TokenHash.JOIN;
      case IntTokenHash.LEFT:
        return hashArray.getHash(pos) != TokenHash.LEFT;
      case IntTokenHash.RIGHT:
        return hashArray.getHash(pos) != TokenHash.RIGHT;
      case IntTokenHash.FOR:
        return hashArray.getHash(pos) != TokenHash.FOR;
      case IntTokenHash.LOCK:
        return hashArray.getHash(pos) != TokenHash.LOCK;
      case IntTokenHash.ON:
        return hashArray.getHash(pos) != TokenHash.ON;
      case IntTokenHash.OFF:
        return hashArray.getHash(pos) != TokenHash.OFF;
      case IntTokenHash.FROM:
        return hashArray.getHash(pos) != TokenHash.FROM;
      default:
        return true;
    }
  }

  private static void debug(Supplier<String> template, Supplier<String> msg) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(template.get(), msg.get());
    }
  }


  /**
   * Account name syntax is 'user_name'@'host_name'.
   */
  public static int pickSpecifyingAccountNames(int pos, final int arrayCount,
      BufferSQLContext context, HashArray hashArray, ByteArrayView sql) {
    TokenizerUtil.debug(pos, context);
    //todo 捕获 'user_name'
    ++pos;
    if (Tokenizer.AT == hashArray.getType(pos)) {
      TokenizerUtil.debug(pos, context);
      ++pos;
      TokenizerUtil.debug(pos, context);
      //todo 捕获 'host_name'
      ++pos;
    } else {
      //语法错误
    }
    return pos;
  }

  public static int pickColumnList(int pos, final int arrayCount, BufferSQLContext context,
      HashArray hashArray, ByteArrayView sql) {
    //todo 捕获 'column'
    TokenizerUtil.debug(pos, context);
    ++pos;
    int type = hashArray.getType(pos);
    while (Tokenizer.COMMA == type) {
      TokenizerUtil.debug(pos, context);
      ++pos;
      TokenizerUtil.debug(pos, context);
      //todo 捕获 'column'
      type = hashArray.getType(++pos);
    }
    return pos;
  }


  public static void debug(Supplier<String> msg) {
    if (false) {
      LOGGER.debug(msg.get());
    }
  }

  public static void debug(int pos, Tokenizer tokenizer, HashArray hashArray) {
    if (false) {
      LOGGER.debug(tokenizer.sql.getString(hashArray.getPos(pos), hashArray.getSize(pos)));
    }
  }

  public static void debug(int pos, BufferSQLContext context) {
    if (false) {
      HashArray hashArray = context.getHashArray();
      LOGGER.debug(context.getBuffer().getString(hashArray.getPos(pos), hashArray.getSize(pos)));
    }
  }

  public static void debugError(int pos, BufferSQLContext context) {
    if (LOGGER.isDebugEnabled()) {
      HashArray hashArray = context.getHashArray();
      LOGGER.debug(hashArray.getType(pos) + ";" + context.getBuffer()
                                                      .getString(hashArray.getPos(pos),
                                                          hashArray.getSize(pos)) + ":" + hashArray
                                                                                              .getHash(
                                                                                                  pos));
    }
  }

}

package io.mycat.sqlparser.util;

import static io.mycat.sqlparser.util.TokenizerUtil.debug;

import io.mycat.MycatExpection;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLIsolationLevel;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jamie on 2017/8/31.
 */
public class TCLSQLParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(TCLSQLParser.class);

  /**
   * @param pos pos已经指向 commit 或者rollback 的后一个字符
   */

  public static int pickSetAutocommit(int pos, final int arrayCount, BufferSQLContext context,
      HashArray hashArray, ByteArrayView sql) {
    context.setSQLType(BufferSQLContext.SET_AUTOCOMMIT_SQL);
    if (hashArray.getType(pos) == Tokenizer.EQUAL) {
      pos++;
      if (hashArray.getType(pos) == Tokenizer.DIGITS) {
        int n = TokenizerUtil.pickNumber(pos, hashArray, sql);
        if (n == 1) {
          context.setAutocommit(true);
        } else {
          context.setAutocommit(false);
        }
        pos++;
        return pos;
      } else if (hashArray.getHash(pos) == TokenHash.ON) {
        context.setAutocommit(true);
        pos++;
        return pos;
      } else if (hashArray.getHash(pos) == TokenHash.OFF) {
        context.setAutocommit(false);
        pos++;
        return pos;
      }
    }
    return pos;
  }

  static int pickStartTransaction(int pos, final int arrayCount, BufferSQLContext context,
      HashArray hashArray) {
    debug(() -> "TRANSACTION");
    //todo testStartTransaction
    while (pos < arrayCount) {
      if (hashArray.getHash(pos) == TokenHash.WITH) {
        pos++;
        debug(() -> "WITH");
        if (hashArray.getHash(pos) == TokenHash.CONSISTENT) {
          pos++;
          debug(() -> "CONSISTENT");
          if (hashArray.getHash(pos) == TokenHash.SNAPSHOT) {
            pos++;
            debug(() -> "SNAPSHOT");
            //todo  testStartTransactionWithConsistentSnapshot
          }
        }
      } else if (hashArray.getHash(pos) == TokenHash.READ) {
        pos++;
        debug(() -> "READ");
        long hash;
        if ((hash = hashArray.getHash(pos)) == TokenHash.ONLY) {
          debug(() -> "ONLY");
          pos++;
        } else if (hash == TokenHash.WRITE) {
          debug(() -> "WRITE");
          pos++;
        }
      }
      if (hashArray.getType(pos) == Tokenizer.COMMA) {
        debug(() -> "COMMA");
        ++pos;
        continue;
      } else {
        break;
      }
    }
    return pos;
  }

  /**
   * /** LOCK TABLES tbl_name [[AS] alias] lock_type [, tbl_name [[AS] alias] lock_type] ...
   * <p>
   * lock_type: READ [LOCAL] | [LOW_PRIORITY] WRITE
   */
  public static int pickLockTables(int pos, final int arrayCount, BufferSQLContext context,
      HashArray hashArray) {
    while (pos < arrayCount) {
      //  int type = hashArray.getType(pos);
      //记录 tbl_name;
      debug(pos, context);
      ++pos;
      if (hashArray.getIntHash(pos) == IntTokenHash.AS) {
        debug(pos, context);
        ++pos;
      }
      //lock_type
      long hash = hashArray.getHash(pos);
      if (hash != TokenHash.READ && hash != TokenHash.LOW_PRIORITY && hash != TokenHash.WRITE) {
        //todo 记录 tbl_name 逻辑优化
        debug(pos, context);
        ++pos;
        hash = hashArray.getHash(pos);
      }
      if (hash == TokenHash.READ) {
        debug(pos, context);
        ++pos;
        if (hashArray.getHash(pos) == TokenHash.LOCAL) {
          debug(pos, context);
          ++pos;
        }
      } else {
        if (hashArray.getHash(pos) == TokenHash.LOW_PRIORITY) {
          debug(pos, context);
          ++pos;
        }
        if (hashArray.getHash(pos) == TokenHash.WRITE) {
          debug(pos, context);
          ++pos;
        }
      }
      int type = hashArray.getType(pos);
      if (type == Tokenizer.COMMA) {
        debug(pos, context);
        ++pos;
        continue;
      } else {
        break;
      }
    }
    ++pos;
    return pos;
  }

  public static int pickSetAutocommitAndSetTransactionOrCharset(int pos, final int arrayCount,
      BufferSQLContext context, HashArray hashArray, ByteArrayView sql) {
    debug(() -> "SET");
    long hash = hashArray.getHash(pos);
    if (hash == TokenHash.AUTOCOMMIT) {
      pos = TCLSQLParser.pickSetAutocommit(++pos, arrayCount, context, hashArray, sql);
      return pos;
    } else if (hash == TokenHash.GLOBAL || hash == TokenHash.SESSION
                   || hash == TokenHash.TRANSACTION) {
      context.setSQLType(BufferSQLContext.SET_TRANSACTION_SQL);
      if (hash == TokenHash.GLOBAL || hash == TokenHash.SESSION) {
        //todo 记录SQL TYPE
        if (hash == TokenHash.GLOBAL) {
          context.setTransactionLevel(MySQLIsolationLevel.GLOBAL);
        } else {
          context.setTransactionLevel(MySQLIsolationLevel.SESSION);
        }
        debug(pos, context);
        ++pos;
        hash = hashArray.getHash(pos);
      }
      debug(pos, context);
      if (hash == TokenHash.TRANSACTION) {
        //todo 记录SQL TYPE
        ++pos;
        while (pos < arrayCount) {
          hash = hashArray.getHash(pos);
          if (hash == TokenHash.READ) {

            debug(pos, context);
            ++pos;
            hash = hashArray.getHash(pos);
            if (hash == TokenHash.WRITE) {
              context.setAccessMode(true);
              debug(pos, context);
              //todo READ WRITE 记录SQL TYPE
              ++pos;
            } else if (hash == TokenHash.ONLY) {
              context.setAccessMode(true);
              debug(pos, context);
              //todo READ ONLY 记录SQL TYPE
              ++pos;
            }
          } else if (hash == TokenHash.ISOLATION) {
            debug(pos, context);
            ++pos;
            if (hashArray.getHash(pos) == TokenHash.LEVEL) {
              debug(pos, context);
              ++pos;
              hash = hashArray.getHash(pos);
              if (hash == TokenHash.REPEATABLE) {
                debug(pos, context);

                ++pos;
                hash = hashArray.getHash(pos);
                if (hash == TokenHash.READ) {
                  debug(pos, context);
                  ++pos;
                  context.setIsolation(MySQLIsolation.REPEATED_READ);
                  //todo  REPEATABLE READ记录SQL TYPE
                }
              } else if (hash == TokenHash.READ) {
                debug(pos, context);
                ++pos;
                hash = hashArray.getHash(pos);
                if (hash == TokenHash.COMMITTED) {
                  debug(pos, context);
                  ++pos;
                  context.setIsolation(MySQLIsolation.READ_COMMITTED);
                  //todo READ COMMITTED 记录SQL TYPE
                } else if (hash == TokenHash.UNCOMMITTED) {
                  debug(pos, context);
                  ++pos;
                  //todo  READ UNCOMMITTED 记录SQL TYPE
                  context.setIsolation(MySQLIsolation.READ_UNCOMMITTED);
                }

              } else if (hash == TokenHash.SERIALIZABLE) {
                debug(pos, context);
                ++pos;
                //todo  SERIALIZABLE 记录SQL TYPE
                context.setIsolation(MySQLIsolation.SERIALIZABLE);
              }
            }
          }
          if (hashArray.getType(pos) == Tokenizer.COMMA) {
            debug(pos, context);
            ++pos;
            continue;
          } else {
            break;
          }
        }
      }
    } else if (hash == TokenHash.NAMES) {
      ++pos;
      context.setSQLType(BufferSQLContext.SET_CHARSET);
      context.setCharset(context.getTokenString(pos));
    } else if (hash == TokenHash.CHARACTER_SET_RESULT) {
      ++pos;
      if((hashArray.getType(pos)!=Tokenizer.EQUAL)){
        throw new MycatExpection("unsupport sql:"+sql.getString(0,sql.length()));
      }
      ++pos;
      context.setCharsetSetResult(context.getTokenString(pos));
      context.setSQLType(BufferSQLContext.SET_CHARSET_RESULT);
    } else {
      //TODO 其他SET 命令支持
      context.setSQLType(BufferSQLContext.SET_SQL);
    }
    return pos;
  }

  /**
   * xid: gtrid [, bqual [, formatID ]]
   */
  public static int pickXid(int pos, final int arrayCount, BufferSQLContext context,
      HashArray hashArray) {
    //todo 保存 gtrid
    debug(pos, context);
    ++pos;
    if (hashArray.getType(pos) == Tokenizer.COMMA) {
      debug(pos, context);
      ++pos;
      //todo 保存 bqual
      debug(pos, context);
      ++pos;
      if (hashArray.getType(pos) == Tokenizer.COMMA) {
        debug(pos, context);
        ++pos;
        //todo 保存 formatID
        debug(pos, context);
        ++pos;
      }
    }
    return pos;
  }

  public static int pickXATransaction(int pos, final int arrayCount, BufferSQLContext context,
      HashArray hashArray) {
    int intHash = hashArray.getIntHash(pos);
    switch (intHash) {
      case IntTokenHash.START:
      case IntTokenHash.BEGIN: {
        debug(pos, context);
        pos = TCLSQLParser.pickXid(++pos, arrayCount, context, hashArray);
        long hash = hashArray.getHash(pos);
        if (hash == TokenHash.JOIN) {
          debug(pos, context);
          ++pos;
          //todo XA {START|BEGIN} xid JOIN 记录SQL_TYPE
        } else if (hash == TokenHash.RESUME) {
          debug(pos, context);
          ++pos;
          //todo XA {START|BEGIN} xid RESUME 记录SQL_TYPE

        }
        break;
      }
      case IntTokenHash.END: {
        debug(pos, context);
        pos = TCLSQLParser.pickXid(++pos, arrayCount, context, hashArray);
        long hash = hashArray.getHash(pos);
        if (hash == TokenHash.SUSPEND) {
          debug(pos, context);
          ++pos;
          hash = hashArray.getHash(pos);
          if (hash == TokenHash.FOR) {
            debug(pos, context);
            ++pos;
            hash = hashArray.getHash(pos);
            if (hash == TokenHash.MIGRATE) {
              debug(pos, context);
              ++pos;
              //todo   标记 XA END xid SUSPEND FOR MIGRATE
            }
          }
        } else {
          //todo   标记 XA END xid
        }
        break;
      }
      case IntTokenHash.PREPARE: {
        debug(pos, context);
        pos = TCLSQLParser.pickXid(++pos, arrayCount, context, hashArray);
        //todo   标记   XA PREPARE xid
        break;
      }
      case IntTokenHash.COMMIT: {
        debug(pos, context);
        pos = TCLSQLParser.pickXid(++pos, arrayCount, context, hashArray);
        if (hashArray.getHash(pos) == TokenHash.ONE) {
          debug(pos, context);
          ++pos;
          if (hashArray.getHash(pos) == TokenHash.PHASE) {
            debug(pos, context);
            ++pos;
            //todo   标记      XA COMMIT xid [ONE PHASE]
          }
        } else {
          //todo   标记     XA COMMIT xid
        }
        break;
      }
      case IntTokenHash.ROLLBACK: {
        debug(pos, context);
        pos = TCLSQLParser.pickXid(++pos, arrayCount, context, hashArray);
        //todo   标记        XA ROLLBACK xid
        break;
      }
      case IntTokenHash.RECOVER: {
        debug(pos, context);
        ++pos;
        if (hashArray.getHash(pos) == TokenHash.CONVERT) {
          debug(pos, context);
          ++pos;
          if (hashArray.getHash(pos) == TokenHash.XID) {
            debug(pos, context);
            //todo   标记            XA RECOVER CONVERT XID
            ++pos;
          }
        } else {
          //todo   标记                 XA RECOVER
        }
        break;
      }
      default:
        break;
    }
    return pos;
  }

}

package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.SQLParseUtils.Tokenizer;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;
import io.mycat.mycat2.sqlparser.byteArrayInterface.Tokenizer2;
import io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil.debug;

/**
 * Created by jamie on 2017/8/31.
 */
public class TCLSQLParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(TCLSQLParser.class);

    /**
     * @param pos        pos已经指向 commit 或者rollback 的后一个字符
     * @param arrayCount
     * @param context
     * @return
     */
    public static int pickCommitRollback(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray) {
        if (hashArray.getHash(pos) == TokenHash.WORK) {
            ++pos;
            debug(() -> "WORK");
        }
        if (hashArray.getHash(pos) == TokenHash.AND) {
            debug(() -> "AND");
            ++pos;
            //todo 逻辑优化
            if (hashArray.getHash(pos) == TokenHash.NO) {
                ++pos;
                debug(() -> "NO");
                if (hashArray.getHash(pos) == TokenHash.CHAIN) {
                    ++pos;
                    debug(() -> "CHAIN");
                    //todo  .setChain(Boolean.FALSE);
                }
            } else {
                if (hashArray.getHash(pos) == TokenHash.CHAIN) {
                    debug(() -> "CHAIN");
                    ++pos;
                    //todo  .setChain(Boolean.TRUE);
                }
            }
        }
        //todo 逻辑优化
        if (hashArray.getHash(pos) == TokenHash.NO) {
            ++pos;
            debug(() -> "NO");
            if (hashArray.getHash(pos) == TokenHash.RELEASE) {
                debug(() -> "RELEASE");
                ++pos;
            }
        } else if (hashArray.getHash(pos) == TokenHash.RELEASE) {
            debug(() -> "RELEASE");
            ++pos;
        }
        //ROLLBACK [WORK] TO [SAVEPOINT] identifier
        if (hashArray.getHash(pos) == TokenHash.TO) {
            debug(pos,context);
            ++pos;
            if (hashArray.getHash(pos) == TokenHash.SAVEPOINT) {
                debug(pos,context);
                ++pos;
                //todo 记录 SAVEPOINT
            }
            //todo 记录 identifier
            debug(pos, context);
            ++pos;
        }
        return pos;
    }

    public static int pickSetAutocommit(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        debug(() -> "AUTOCOMMIT");
        if (hashArray.getType(pos) == Tokenizer2.EQUAL) {
            debug(() -> "=");
            pos++;
            if (hashArray.getType(pos) == Tokenizer2.DIGITS) {
                int n = TokenizerUtil.pickNumber(pos, hashArray, sql);
                if (n == 1) {
                    debug(() -> "1");
                } else {
                    debug(() -> "0");
                }
                //todo 设置
                pos++;
            }
        }
        return pos;
    }

    static int pickStartTransaction(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray) {
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
            if (hashArray.getType(pos) == Tokenizer2.COMMA) {
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
     * /**
     * LOCK TABLES
     * tbl_name [[AS] alias] lock_type
     * [, tbl_name [[AS] alias] lock_type] ...
     * <p>
     * lock_type:
     * READ [LOCAL]
     * | [LOW_PRIORITY] WRITE
     *
     * @param pos
     * @param arrayCount
     * @param context
     * @param hashArray
     * @return
     */
    public static int pickLockTables(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray) {
        while (pos < arrayCount) {
          //  int type = hashArray.getType(pos);
            //记录 tbl_name;
            TokenizerUtil.debug(pos, context);
            ++pos;
            if (hashArray.getIntHash(pos) == IntTokenHash.AS) {
                debug(pos,context);
                ++pos;
            }
            //lock_type
            long hash = hashArray.getHash(pos);
            if (hash !=TokenHash. READ && hash != TokenHash. LOW_PRIORITY && hash !=TokenHash.  WRITE) {
                //todo 记录 tbl_name 逻辑优化
                TokenizerUtil.debug(pos, context);
                ++pos;
                hash=hashArray.getHash(pos);
            }
            if (hash == TokenHash. READ) {
                debug(pos,context);
                ++pos;
                if (hashArray.getHash(pos) ==TokenHash. LOCAL) {
                    TokenizerUtil.debug(pos, context);
                    ++pos;
                }
            } else {
                if (hashArray.getHash(pos) == TokenHash.LOW_PRIORITY) {
                    TokenizerUtil.debug(pos, context);
                    ++pos;
                }
                if (hashArray.getHash(pos) ==TokenHash. WRITE) {
                    TokenizerUtil.debug(pos, context);
                    ++pos;
                }
            }
           int type=hashArray.getType(pos);
            if (type == Tokenizer.COMMA) {
                TokenizerUtil.debug(pos, context);
                ++pos;
                continue;
            } else {
                break;
            }
        }
        ++pos;
        return pos;
    }

    public static int pickSetAutocommitAndSetTransaction(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        context.setSQLType(BufferSQLContext.SET_SQL);
        TokenizerUtil.debug(() -> "SET");
        long hash = hashArray.getHash(pos);
        if (hash == TokenHash.AUTOCOMMIT) {
            pos = TCLSQLParser.pickSetAutocommit(++pos, arrayCount, context, hashArray, sql);
            return pos;
        } else if (hash == TokenHash.GLOBAL || hash == TokenHash.SESSION || hash ==  TokenHash.TRANSACTION) {
            if (hash == TokenHash.GLOBAL || hash == TokenHash.SESSION) {
                //todo 记录SQL TYPE
                debug(pos, context);
                ++pos;
                hash=hashArray.getHash(pos);
            }
            debug(pos, context);
            if (hash == TokenHash.TRANSACTION) {
                //todo 记录SQL TYPE
                ++pos;
                while (pos < arrayCount) {
                    hash = hashArray.getHash(pos);
                    if (hash == TokenHash. READ) {
                        debug(pos, context);
                        ++pos;
                        hash = hashArray.getHash(pos);
                        if (hash == TokenHash.WRITE) {
                            debug(pos, context);
                            //todo READ WRITE 记录SQL TYPE
                            ++pos;
                        } else if (hash == TokenHash.ONLY) {
                            debug(pos, context);
                            //todo READ ONLY 记录SQL TYPE
                            ++pos;
                        }
                    } else if (hash ==  TokenHash.ISOLATION) {
                        debug(pos, context);
                        ++pos;
                        if (hashArray.getHash(pos) == TokenHash. LEVEL) {
                            debug(pos, context);
                            ++pos;
                            hash = hashArray.getHash(pos);
                            if (hash == TokenHash. REPEATABLE) {
                                debug(pos, context);
                                ++pos;
                                hash = hashArray.getHash(pos);
                                if (hash ==  TokenHash.READ) {
                                    debug(pos, context);
                                    ++pos;
                                    //todo  REPEATABLE READ记录SQL TYPE
                                }
                            } else if (hash ==  TokenHash.READ) {
                                debug(pos, context);
                                ++pos;
                                hash = hashArray.getHash(pos);
                                if (hash == TokenHash. COMMITTED) {
                                    debug(pos, context);
                                    ++pos;
                                    //todo READ COMMITTED 记录SQL TYPE
                                } else if (hash == TokenHash. UNCOMMITTED) {
                                    debug(pos, context);
                                    ++pos;
                                    //todo  READ UNCOMMITTED 记录SQL TYPE
                                }

                            } else if (hash ==  TokenHash.SERIALIZABLE) {
                                debug(pos, context);
                                ++pos;
                                //todo  SERIALIZABLE 记录SQL TYPE
                            }
                        }
                    }
                    if (hashArray.getType(pos) == Tokenizer2.COMMA) {
                        debug(pos, context);
                        ++pos;
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }
        return pos;
    }

    /**
     *      xid: gtrid [, bqual [, formatID ]]
     * @param pos
     * @param arrayCount
     * @param context
     * @param hashArray
     * @return
     */
    public static int pickXid(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray) {
        //todo 保存 gtrid
        debug(pos, context);
        ++pos;
        if (hashArray.getType(pos) == Tokenizer2.COMMA) {
            debug(pos, context);
            ++pos;
            //todo 保存 bqual
            debug(pos, context);
            ++pos;
            if (hashArray.getType(pos) == Tokenizer2.COMMA) {
                debug(pos, context);
                ++pos;
                //todo 保存 formatID
                debug(pos, context);
                ++pos;
            }
        }
        return pos;
    }

    public static int pickXATransaction(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray) {
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
                } else if (hash == TokenHash. RESUME) {
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
                if (hash == TokenHash. SUSPEND) {
                    debug(pos, context);
                    ++pos;
                    hash = hashArray.getHash(pos);
                    if (hash == TokenHash. FOR) {
                        debug(pos, context);
                        ++pos;
                        hash = hashArray.getHash(pos);
                        if (hash == TokenHash. MIGRATE) {
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
                if (hashArray.getHash(pos) == TokenHash. ONE) {
                    debug(pos, context);
                    ++pos;
                    if (hashArray.getHash(pos) == TokenHash. PHASE) {
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
                if (hashArray.getHash(pos) ==  TokenHash.CONVERT) {
                    debug(pos, context);
                    ++pos;
                    if (hashArray.getHash(pos) ==  TokenHash.XID) {
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

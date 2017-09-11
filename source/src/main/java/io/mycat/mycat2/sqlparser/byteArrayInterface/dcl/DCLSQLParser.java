package io.mycat.mycat2.sqlparser.byteArrayInterface.dcl;

import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.TokenHash;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.byteArrayInterface.Tokenizer2;
import io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil;

/**
 * Created by jamie on 2017/8/31.
 */
public class DCLSQLParser {

    /**
     * 前置条件 当前pos指向grant的下一个token
     */
    public static int pickGrant(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.PROXY == longHash) {
            TokenizerUtil.debug(pos, context);
            return pickGrantProxy(++pos, arrayCount, context, hashArray, sql);
        } else {
            return pickGrantPrivType(pos, arrayCount, context, hashArray, sql);
        }
    }

    public static int pickRevoke(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.PROXY == longHash) {
            TokenizerUtil.debug(pos, context);
            return pickRevokeProxy(++pos, arrayCount, context, hashArray, sql);
        } else if (TokenHash.ALL == longHash) {
            TokenizerUtil.debug(pos, context);
            return pickRevokeAll(++pos, arrayCount, context, hashArray, sql);
        } else {
            return pickRevokePrivType(pos, arrayCount, context, hashArray, sql);
        }
    }

    /**
     * GRANT
     * priv_type [(column_list)]
     * [, priv_type [(column_list)]] ...
     * ON [object_type] priv_level
     * TO user [auth_option] [, user [auth_option]] ...
     * [REQUIRE {NONE | tls_option [[AND] tls_option] ...}]
     * [WITH {GRANT OPTION | resource_option} ...]
     * <p>
     * GRANT PROXY ON user
     * TO user [, user] ...
     * [WITH GRANT OPTION]
     **/
    public static int pickGrantProxy(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.ON == longHash) {
            TokenizerUtil.debug(pos, context);
            //todo 捕获 user
            pos = DCLSQLParserHelper.pickUser(++pos, arrayCount, context, hashArray, sql);
            TokenizerUtil.debug(pos, context);
            longHash = hashArray.getHash(pos);
            if (TokenHash.TO == longHash) {
                ++pos;
                pos = DCLSQLParserHelper.pickUser(pos, arrayCount, context, hashArray, sql);
                int type = hashArray.getType(pos);
                while (type == Tokenizer2.COMMA) {
                    TokenizerUtil.debug(pos, context);
                    //todo 捕获 user
                    pos = DCLSQLParserHelper.pickUser(++pos, arrayCount, context, hashArray, sql);
                    type = hashArray.getType(pos++);
                }
                longHash = hashArray.getHash(--pos );
                if (TokenHash.WITH == longHash) {
                    TokenizerUtil.debug(()->"WITH");
                    pos ++;
                    longHash = hashArray.getHash(pos);
                    if (TokenHash.GRANT == longHash) {
                        TokenizerUtil.debug(()->"GRANT");
                        longHash = hashArray.getHash(++pos);
                        if (TokenHash.OPTION == longHash) {
                            TokenizerUtil.debug(()->"OPTION");
                            pos++;
                                    /*
                                         todo GRANT PROXY ON user TO user [, user] ...[WITH GRANT OPTION]
                                     */
                            return pos;
                        }
                    }
                    //语法错误
                } else {
                    //todo GRANT PROXY ON user  TO user [, user] ...
                }
            }
        }

        return pos;
    }

    /**
     * GRANT
     * priv_type [(column_list)]
     * [, priv_type [(column_list)]] ...
     * ON [object_type] priv_level
     * TO user [auth_option] [, user [auth_option]] ...
     * [REQUIRE {NONE | tls_option [[AND] tls_option] ...}]
     * [WITH {GRANT OPTION | resource_option} ...]
     * todo 检查一下前置条件
     */
    public static int pickGrantPrivType(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash;
        pos = pickPrivTypeWithColumnList(pos, arrayCount, context, hashArray, sql);
        longHash = hashArray.getHash(pos++);
        if (TokenHash.ON == longHash) {
            if (DCLSQLParserHelper.isObjectType(pos, arrayCount, context, hashArray, sql)) {
                //todo 逻辑优化
                pos = DCLSQLParserHelper.pickObjectType(pos, arrayCount, context, hashArray, sql);
            }
            //todo 捕获 ON [object_type] priv_level中的 priv_level
            pos = DCLSQLParserHelper.pickPrivLevel(pos, arrayCount, context, hashArray, sql);
            //TO user [auth_option] [, user [auth_option]] ...
            longHash = hashArray.getHash(pos);
            TokenizerUtil.debug(pos, context);
            if (TokenHash.TO == longHash) {
                pos = pickToUserAuthOption(pos, arrayCount, context, hashArray, sql);
            }
            //[REQUIRE {NONE | tls_option [[AND] tls_option] ...}]
            TokenizerUtil.debug(pos, context);
            longHash = hashArray.getHash(pos++);
            if (TokenHash.REQUIRE == longHash) {
                pos = pickRequire(pos, arrayCount, context, hashArray, sql);
            }
            //[WITH {GRANT OPTION | resource_option} ...]
            //todo 这一大片代码都要优化
            longHash = hashArray.getHash(pos);
            TokenizerUtil.debug(pos, context);
            if (TokenHash.WITH == longHash) {
                pos = pickWithGrantOptionResourceOption(++pos, arrayCount, context, hashArray, sql);
            }
        }
        return pos;
    }

    /**
     * 前置条件
     * if (TokenHash.TO==longHash){
     * <p>
     * }
     * TO user [auth_option] [, user [auth_option]] ...
     */
    public static int pickToUserAuthOption(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        pos++;
        int type;
        do {
            //todo 捕获 user
            pos = DCLSQLParserHelper.pickUser(pos, arrayCount, context, hashArray, sql);
            //todo 捕获 auth_option
            if (DCLSQLParserHelper.isAuthOption(pos, arrayCount, context, hashArray, sql)) {
                pos = DCLSQLParserHelper.pickAuthOption(pos, arrayCount, context, hashArray, sql);
            }
            TokenizerUtil.debug(pos, context);
            type = hashArray.getType(pos++);
        } while (Tokenizer2.COMMA == type);
        --pos;
        return pos;
    }

    /**
     * [WITH {GRANT OPTION | resource_option} ...]
     */
    public static int pickWithGrantOptionResourceOption(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        TokenizerUtil.debug(pos, context);
        long longHash = hashArray.getHash(pos);
        if (TokenHash.GRANT == longHash) {
            TokenizerUtil.debug(pos, context);
            ++pos;
            longHash = hashArray.getHash(pos);
            if (TokenHash.OPTION == longHash) {
                TokenizerUtil.debug(pos, context);
                //todo GRANT OPTION
                ++pos;
                return pos;
            } else {
                //语法错误
            }
        } else {
            while (DCLSQLParserHelper.isResourceOption(pos, arrayCount, context, hashArray, sql)) {
                //todo 捕获resource_option
                pos = DCLSQLParserHelper.pickResourceOption(pos, arrayCount, context, hashArray, sql);
            }
        }
        return pos;
    }

    public static int pickRequire(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.NONE == longHash) {
            TokenizerUtil.debug(pos, context);
            ++pos;
        } else {
            pos = DCLSQLParserHelper.pickTlsOption(pos, arrayCount, context, hashArray, sql);
            TokenizerUtil.debug(pos, context);
            longHash = hashArray.getHash(pos);
            do {
                if (TokenHash.AND == longHash) {
                    TokenizerUtil.debug(pos, context);
                    pos++;
                    pos = DCLSQLParserHelper.pickTlsOption(pos, arrayCount, context, hashArray, sql);
                    TokenizerUtil.debug(pos, context);
                    longHash=hashArray.getHash(pos);
                    continue;
                } else {
                    if (DCLSQLParserHelper.isTlsOption(pos, arrayCount, context, hashArray, sql)) {
                        pos = DCLSQLParserHelper.pickTlsOption(pos, arrayCount, context, hashArray, sql);
                        TokenizerUtil.debug(pos, context);
                        longHash=hashArray.getHash(pos);
                        continue;
                    } else {
                        break;
                    }
                }
            } while (true);
        }
        return pos;
    }

    public static int pickPrivTypeWithColumnList(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        while (true) {
            pos = DCLSQLParserHelper.pickPrivType(pos, arrayCount, context, hashArray, sql);
            //todo  捕获 priv_type
            int type = hashArray.getType(pos);
            TokenizerUtil.debug(pos, context);
            if (Tokenizer2.LEFT_PARENTHESES == type) {
                pos++;
                do {
                    TokenizerUtil.debug(pos, context);
                    //todo  捕获 column
                    pos++;
                    TokenizerUtil.debug(pos, context);
                } while (Tokenizer2.COMMA == (type = hashArray.getType(pos++)));
                if (Tokenizer2.RIGHT_PARENTHESES == type) {
                    TokenizerUtil.debug(pos, context);
                }
            }
            type = hashArray.getType(pos);
            if (Tokenizer2.COMMA == type) {
//                TokenizerUtil.debug(pos,context);
                ++pos;
                continue;
            } else {
                break;
            }
        }
        return pos;
    }


    /**
     * REVOKE PROXY ON user
     * FROM user [, user] ...
     */
    public static int pickRevokeProxy(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.ON == longHash) {
            TokenizerUtil.debug(()->"ON");
            //todo 捕获 user
            pos = DCLSQLParserHelper.pickUser(++pos, arrayCount, context, hashArray, sql);
            longHash = hashArray.getHash(pos++);
            if (TokenHash.FROM == longHash) {
                TokenizerUtil.debug(()->"FROM");
                pos = DCLSQLParserHelper.pickUser(pos, arrayCount, context, hashArray, sql);
                int type = hashArray.getType(pos++);
                while (type == Tokenizer2.COMMA) {
                    TokenizerUtil.debug(()->",");
                    //todo 捕获 user
                    pos = DCLSQLParserHelper.pickUser(pos, arrayCount, context, hashArray, sql);
                    type = hashArray.getType(pos++);
                }
            }
        }
        return pos;
    }

    /***
     REVOKE ALL [PRIVILEGES], GRANT OPTION
     FROM user [, user] ...
     **/
    public static int pickRevokeAll(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.PRIVILEGES == longHash) {
            TokenizerUtil.debug(pos,context);
            //todo 捕获PRIVILEGES
            pos++;
        }
        int type = hashArray.getType(pos);
        if (Tokenizer2.COMMA == type) {
            pos++;
            longHash = hashArray.getHash(pos++);
            if (TokenHash.GRANT == longHash) {
                longHash = hashArray.getHash(pos++);
                if (TokenHash.OPTION == longHash) {
                    longHash = hashArray.getHash(pos++);
                    if (TokenHash.FROM == longHash) {
                        TokenizerUtil.debug(()->"FROM");
                        //todo 捕获 user
                        pos = DCLSQLParserHelper.pickUser(pos, arrayCount, context, hashArray, sql);
                        type = hashArray.getType(pos++);
                        while (type == Tokenizer2.COMMA) {
                            TokenizerUtil.debug(()->",");
                            //todo 捕获 user
                            pos = DCLSQLParserHelper.pickUser(pos, arrayCount, context, hashArray, sql);
                            type = hashArray.getType(pos++);
                        }
                        return pos;
                    }
                }
            }
        }
        //语法错误
        return pos;
    }

    /**
     * REVOKE
     * priv_type [(column_list)]
     * [, priv_type [(column_list)]] ...
     * ON [object_type] priv_level
     * FROM user [, user] ...
     ***/
    public static int pickRevokePrivType(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash;
        pos = pickPrivTypeWithColumnList(pos, arrayCount, context, hashArray, sql);
        longHash = hashArray.getHash(pos++);
        if (TokenHash.ON == longHash) {
            if (DCLSQLParserHelper.isObjectType(pos, arrayCount, context, hashArray, sql)) {
                //todo 逻辑优化
                pos = DCLSQLParserHelper.pickObjectType(pos, arrayCount, context, hashArray, sql);
            }
            //todo 捕获 ON [object_type] priv_level中的 priv_level
            pos = DCLSQLParserHelper.pickPrivLevel(pos, arrayCount, context, hashArray, sql);

            longHash = hashArray.getHash(pos++);
            //FROM user [, user] ...
            if (TokenHash.FROM == longHash) {
                TokenizerUtil.debug(()->"FROM");
                //todo 捕获 user
                pos = DCLSQLParserHelper.pickUser(pos, arrayCount, context, hashArray, sql);
                int type = hashArray.getType(pos++);
                while (type == Tokenizer2.COMMA) {
                    TokenizerUtil.debug(()->",");
                    //todo 捕获 user
                    pos = DCLSQLParserHelper.pickUser(pos, arrayCount, context, hashArray, sql);
                    type = hashArray.getType(pos++);
                }
                return pos;
            }
        }
        return pos;
    }
}

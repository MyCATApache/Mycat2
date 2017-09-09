package io.mycat.mycat2.sqlparser.byteArrayInterface.dcl;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.BufferSQLParser;
import io.mycat.mycat2.sqlparser.IntTokenHash;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.TokenHash;
import io.mycat.mycat2.sqlparser.byteArrayInterface.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jamie on 2017/9/4.
 */
public class DCLSQLParserHelper {
    public static int pickPrivType(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        int intHash = hashArray.getIntHash(pos);
        long longHash;
        switch (intHash) {
            case IntTokenHash.ALL: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo ALL
                break;
            }
            case IntTokenHash.ALTER: {
                TokenizerUtil.debug(pos,context);
                longHash = hashArray.getHash(++pos);
                if (longHash == TokenHash.ROUTINE) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo ALTER ROUTINE
                } else {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo  ALTER
                }
                break;
            }
            case IntTokenHash.CREATE: {
                TokenizerUtil.debug(pos,context);
                longHash = hashArray.getHash(++pos);
                if (longHash == TokenHash.ROUTINE) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo CREATE ROUTINE
                } else if (longHash == TokenHash.TABLESPACE) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo  CREATE TABLESPACE
                } else if (longHash == TokenHash.TEMPORARY) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    longHash = hashArray.getHash(pos);
                    if (TokenHash.TABLES == longHash) {
                        TokenizerUtil.debug(pos,context);
                        pos++;
                        //todo CREATE TEMPORARY TABLES
                    } else {
                        //todo 语法错误
                    }
                } else if (longHash == TokenHash.USER) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo CREATE USER
                } else if (longHash == TokenHash.VIEW) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo CREATE VIEW
                } else {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo CREATE
                }
                break;
            }
            case IntTokenHash.DELETE: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo DELETE
                break;
            }
            case IntTokenHash.DROP: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo DROP
                break;
            }
            case IntTokenHash.EVENT: {
                TokenizerUtil.debug(pos,context);
                //todo EVENT
                break;
            }
            case IntTokenHash.EXECUTE: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo EXECUTE
                break;
            }
            case IntTokenHash.FILE: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo FILE
                break;
            }
            case IntTokenHash.GRANT: {
                TokenizerUtil.debug(pos,context);
                longHash = hashArray.getHash(++pos);
                if (longHash == TokenHash.OPTION) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo GRANT OPTION
                } else {
                    //语法错误
                }
                break;
            }
            case IntTokenHash.INDEX: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo INDEX
                break;
            }
            case IntTokenHash.INSERT: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo INSERT
                break;
            }
            case IntTokenHash.LOCK: {
                TokenizerUtil.debug(pos,context);
                longHash = hashArray.getHash(++pos);
                if (longHash == TokenHash.TABLES) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo LOCK TABLES
                } else {
                    //语法错误
                }
                break;
            }
            case IntTokenHash.PROCESS: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo PROCESS
                break;
            }
            case IntTokenHash.PROXY: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo PROXY
                break;
            }
            case IntTokenHash.REFERENCES: {
                TokenizerUtil.debug(pos,context);
                //todo REFERENCES
                break;
            }
            case IntTokenHash.RELOAD: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo RELOAD
                break;
            }
            case IntTokenHash.REPLICATION: {
                TokenizerUtil.debug(pos,context);
                longHash = hashArray.getHash(++pos);
                if (longHash == TokenHash.CLIENT) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo REPLICATION CLIENT
                } else if (longHash == TokenHash.SLAVE) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo REPLICATION SLAVE
                } else {
                    //语法错误
                }
                break;
            }

            case IntTokenHash.SELECT: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo SELECT
                break;
            }
            case IntTokenHash.SHOW: {
                TokenizerUtil.debug(pos,context);
                longHash = hashArray.getHash(++pos);
                if (longHash == TokenHash.DATABASES) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo SHOW DATABASES
                } else if (longHash == TokenHash.VIEW) {
                    TokenizerUtil.debug(pos,context);
                    pos++;
                    //todo SHOW VIEW
                } else {
                    //语法错误
                }
                break;
            }
            case IntTokenHash.SHUTDOWN: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo SHUTDOWN
                break;
            }
            case IntTokenHash.SUPER: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo SUPER
                break;
            }
            case IntTokenHash.TRIGGER: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo TRIGGER
                break;
            }
            case IntTokenHash.UPDATE: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo UPDATE
                break;
            }
            case IntTokenHash.USAGE: {
                TokenizerUtil.debug(pos,context);
                pos++;
                //todo USAGE
                break;
            }
            default: {
                //语法错误
                break;
            }
        }
        return pos;
    }
    public static boolean isPrivType(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        int intHash = hashArray.getIntHash(pos);
        long longHash;
        switch (intHash) {
            case IntTokenHash.ALL: {
                return true;
            }
            case IntTokenHash.ALTER: {
                longHash = hashArray.getHash(pos);
                if (longHash == TokenHash.ROUTINE) {
                    return true;
                    //todo ALTER ROUTINE
                } else {
                    return true;
                    //todo  ALTER
                }
            }
            case IntTokenHash.CREATE: {
                longHash = hashArray.getHash(pos);
                if (longHash == TokenHash.ROUTINE) {
                    return true;
                } else if (longHash == TokenHash.TABLESPACE) {
                    return true;
                } else if (longHash == TokenHash.TEMPORARY) {
                    pos++;
                    longHash = hashArray.getHash(pos);
                    if (TokenHash.TABLES == longHash) {
                        return true;
                    } else {
                     return false;
                    }
                } else if (longHash == TokenHash.USER) {
                    return true;
                } else if (longHash == TokenHash.VIEW) {
                    return true;
                } else {
                    return true;
                }
            }
            case IntTokenHash.DELETE: {
                return true;
            }
            case IntTokenHash.DROP: {
                return true;
            }
            case IntTokenHash.EVENT: {
                return true;
            }
            case IntTokenHash.EXECUTE: {
                return true;
            }
            case IntTokenHash.FILE: {
                return true;
            }
            case IntTokenHash.GRANT: {
                longHash = hashArray.getHash(pos);
                if (longHash == TokenHash.OPTION) {
                    return true;
                } else {
                    return false;
                }
            }
            case IntTokenHash.INDEX: {
                return true;
            }
            case IntTokenHash.INSERT: {
                return true;
            }
            case IntTokenHash.LOCK: {
                longHash = hashArray.getHash(pos);
                if (longHash == TokenHash.TABLES) {
                    return true;
                } else {
                    return false;
                }
            }
            case IntTokenHash.PROCESS: {
                return true;
            }
            case IntTokenHash.PROXY: {
                return true;
            }
            case IntTokenHash.REFERENCES: {
                return true;
            }
            case IntTokenHash.RELOAD: {
                return true;
            }
            case IntTokenHash.REPLICATION: {
                longHash = hashArray.getHash(pos);
                if (longHash == TokenHash.CLIENT) {
                    return true;
                } else if (longHash == TokenHash.SLAVE) {
                    return true;
                } else {
                    return false;
                }
            }

            case IntTokenHash.SELECT: {
                return true;
            }
            case IntTokenHash.SHOW: {
                longHash = hashArray.getHash(pos);
                if (longHash == TokenHash.DATABASES) {
                    return true;
                } else if (longHash == TokenHash.VIEW) {
                    return true;
                } else {
                    return false;
                }
            }
            case IntTokenHash.SHUTDOWN: {
                return true;
            }
            case IntTokenHash.SUPER: {
                return true;
            }
            case IntTokenHash.TRIGGER: {
                return true;
            }
            case IntTokenHash.UPDATE: {
                return true;
            }
            case IntTokenHash.USAGE: {
                return true;
            }
            default: {
                return false;
            }
        }
    }

    /**
    tls_option: {
     * SSL
     * | X509
     * | CIPHER 'cipher'
     * | ISSUER 'issuer'
     * | SUBJECT 'subject'
     * }
     */
    public static int pickTlsOption(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.SSL == longHash) {
            //todo SSL
            TokenizerUtil.debug(pos,context);
            ++pos;
        } else if (TokenHash.X509 == longHash) {
            //todo X509
            TokenizerUtil.debug(pos,context);
            ++pos;
        }else if (TokenHash.CIPHER == longHash) {
            TokenizerUtil.debug(pos,context);
            ++pos;
            //todo CIPHER 'cipher'  是否 'cipher'
            TokenizerUtil.debug(pos,context);
            ++pos;
        } else if (TokenHash.ISSUER == longHash) {
            TokenizerUtil.debug(pos,context);
            ++pos;
            //todo ISSUER 'issuer' 捕获  是否 'issuer'
            TokenizerUtil.debug(pos,context);
            ++pos;
        }else if (TokenHash.SUBJECT == longHash) {
            TokenizerUtil.debug(pos,context);
            ++pos;
            //todo SUBJECT 'subject' 捕获  是否 'subject'
            TokenizerUtil.debug(pos,context);
            ++pos;
        }
        return pos;
    }
    public static boolean isTlsOption(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.SSL == longHash) {
            //todo SSL
            return true;
        } else if (TokenHash.X509 == longHash) {
            //todo X509
            return true;
        }else if (TokenHash.CIPHER == longHash) {
            ++pos;
            //todo CIPHER 'cipher'  是否 'cipher'
            return true;
        } else if (TokenHash.ISSUER == longHash) {
            ++pos;
            //todo ISSUER 'issuer' 捕获  是否 'issuer'
            return true;
        }else if (TokenHash.SUBJECT == longHash) {
            ++pos;
            //todo SUBJECT 'subject' 捕获  是否 'subject'
            return true;
        }
        return false;

    }

    /**
     *
     *     resource_option: {
    | MAX_QUERIES_PER_HOUR count
    | MAX_UPDATES_PER_HOUR count
    | MAX_CONNECTIONS_PER_HOUR count
    | MAX_USER_CONNECTIONS count
    }
     */
    public static int pickResourceOption(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.MAX_QUERIES_PER_HOUR == longHash) {
            TokenizerUtil.debug(pos,context);
            ++pos;
            //todo MAX_QUERIES_PER_HOUR

        } else if (TokenHash.MAX_UPDATES_PER_HOUR == longHash) {
            TokenizerUtil.debug(pos,context);
            ++pos;
            //todo MAX_UPDATES_PER_HOUR

        }else if (TokenHash.MAX_CONNECTIONS_PER_HOUR == longHash) {
            TokenizerUtil.debug(pos,context);
            ++pos;
            //todo MAX_CONNECTIONS_PER_HOUR

        } else if (TokenHash.MAX_USER_CONNECTIONS == longHash) {
            TokenizerUtil.debug(pos,context);
            ++pos;
            //todo MAX_USER_CONNECTIONS

        }else {
            //语法错误
        }
        TokenizerUtil.debug(pos,context);
       int count= TokenizerUtil.pickNumber(pos,hashArray,sql);
        ++pos;
        return pos;
    }
    public static boolean isResourceOption(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.MAX_QUERIES_PER_HOUR == longHash) {
          return true;
        } else if (TokenHash.MAX_UPDATES_PER_HOUR == longHash) {
            return true;
        }else if (TokenHash.MAX_CONNECTIONS_PER_HOUR == longHash) {
            return true;
        } else if (TokenHash.MAX_USER_CONNECTIONS == longHash) {
            return true;
        }
        return false;
    }

    /**
     *
     auth_option: {
     IDENTIFIED BY 'auth_string'
     | IDENTIFIED WITH auth_plugin
     | IDENTIFIED WITH auth_plugin BY 'auth_string'
     | IDENTIFIED WITH auth_plugin AS 'hash_string'
     | IDENTIFIED BY PASSWORD 'hash_string'
     }
     */
    public static int pickAuthOption(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
       //todo 捕获 IDENTIFIED
        TokenizerUtil.debug(pos,context);
        ++pos;
        long longHash=hashArray.getHash(pos);
        if (TokenHash.BY == longHash) {
            TokenizerUtil.debug(pos,context);
            ++pos;
            if (TokenHash.PASSWORD==hashArray.getHash(pos)){
                TokenizerUtil.debug(pos,context);
                ++pos;
                TokenizerUtil.debug(pos,context);
              //todo  IDENTIFIED BY PASSWORD 'hash_string'
                //todo 捕获 'hash_string'
                ++pos;
            }else {
                TokenizerUtil.debug(pos,context);
               //todo  IDENTIFIED BY 'auth_string'
                //todo 捕获'auth_string'
                ++pos;
            }
        } if (TokenHash.WITH == longHash) {
            TokenizerUtil.debug(pos,context);
            ++pos;
            TokenizerUtil.debug(pos,context);//todo 捕获 auth_plugin
            ++pos;
            longHash=hashArray.getHash(pos);
            if (TokenHash.BY ==longHash){
                TokenizerUtil.debug(pos,context);
                ++pos;
                TokenizerUtil.debug(pos,context);
                //todo IDENTIFIED WITH auth_plugin BY 'auth_string' 捕获 'auth_string'
                ++pos;
            }else if (TokenHash.AS ==longHash){
                TokenizerUtil.debug(pos,context);
                ++pos;
                TokenizerUtil.debug(pos,context);
                //todo IDENTIFIED WITH auth_plugin AS 'hash_string' 捕获 'hash_string'
                ++pos;
            }else {
                //语法错误
            }
        }else {
            //语法错误
        }
        return pos;
    }
    public static boolean isAuthOption(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        //todo 捕获 IDENTIFIED
        ++pos;
        long longHash=hashArray.getHash(pos);
        TokenizerUtil.debug(pos,context);
        if (TokenHash.BY == longHash) {
            ++pos;
            if (TokenHash.PASSWORD==hashArray.getHash(pos)){
                //todo  IDENTIFIED BY PASSWORD 'hash_string'
                //todo 捕获 'hash_string'
                return true;
            }else {

                //todo  IDENTIFIED BY 'auth_string'
                //todo 捕获'auth_string'
                return true;
            }
        } if (TokenHash.WITH == longHash) {
            /*
                 auth_option: {
     IDENTIFIED BY 'auth_string'
     | IDENTIFIED WITH auth_plugin
     | IDENTIFIED WITH auth_plugin BY 'auth_string'
     | IDENTIFIED WITH auth_plugin AS 'hash_string'
     | IDENTIFIED BY PASSWORD 'hash_string'
     }
             */
            ++pos;
            //skip auth_plugin
            ++pos;
            longHash=hashArray.getHash(pos);
            if (TokenHash.BY ==longHash){
                return true;
            }else if (TokenHash.AS ==longHash){
                return true;
            }else {
                //语法错误
            }
        }else {
            //语法错误
        }
        return false;
    }
    /**
     *
     object_type: {
     TABLE
     | FUNCTION
     | PROCEDURE
     }
     */
    public static int pickObjectType(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.TABLE == longHash) {
            //todo 捕获 TABLE
            TokenizerUtil.debug(pos,context);
            ++pos;
        } else if (TokenHash.FUNCTION == longHash) {
            TokenizerUtil.debug(pos,context);
            //todo 捕获 FUNCTION
            ++pos;
        }else if (TokenHash.PROCEDURE == longHash) {
            TokenizerUtil.debug(pos,context);
            //todo 捕获 PROCEDURE
            ++pos;
        } else{
            //语法错误
        }
        return pos;
    }
    public static boolean isObjectType(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        long longHash = hashArray.getHash(pos);
        if (TokenHash.TABLE == longHash) {
           return true;
        } else if (TokenHash.FUNCTION == longHash) {
            return true;
        }else if (TokenHash.PROCEDURE == longHash) {
            return true;
        }
        return false;
    }

    /**
     *
     priv_level: {
     *
     | *.*
     | db_name.*
     | db_name.tbl_name
     | tbl_name
     | db_name.routine_name
     }
     */
    public static int pickPrivLevel(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        int type = hashArray.getType(pos);
        if (Tokenizer2.STAR==type){
            type = hashArray.getType(pos+1);
            if (Tokenizer2.DOT==type){
              //  TokenizerUtil.debug(pos,context);
                type = hashArray.getType(pos+2);
                if (Tokenizer2.STAR==type){
                    TokenizerUtil.debug(()->"*.*");
                    //todo *.*
                   return pos+3;
                }else {
                    //语法错误
                }
            }else {
                TokenizerUtil.debug(()->"*");
                //todo *
                return pos+1;
            }
        }else {
            type = hashArray.getType(pos+1);
            if (Tokenizer2.DOT==type){
                TokenizerUtil.debug(pos+1,context);
                //todo 捕获 db_name 此时pos即是db_name的位置
                TokenizerUtil.debug(pos+2,context);
                //todo 捕获 db_name 此时pos+2即是tbl_name或者routine_name的位置
            return pos+3;
            }else {
                TokenizerUtil.debug(pos,context);
                //todo 捕获 tbl_name 此时pos即是tbl_name的位置
                return pos+1;
            }
        }

        return pos;
    }
    /**
     * Account name syntax is 'user_name'@'host_name'.
     */
    public static int pickUser(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql) {
        return TokenizerUtil.pickSpecifyingAccountNames(pos, arrayCount, context, hashArray, sql);
    }

    public static void main(String[] args) throws Exception {
        BufferSQLParser parser = new BufferSQLParser();
        BufferSQLContext context = new BufferSQLContext();
        Files.lines(Paths.get("D:\\SQLparserNew\\src\\main\\resources\\privileges.txt"))
                .forEach((i) -> System.out.println(String.format("  case IntTokenHash.%s: {\n" +
                        "                break;\n" +
                        "            }", i)));
        Map<Boolean, List<String>> map = Files.lines(Paths.get("D:\\SQLparserNew\\src\\main\\resources\\privileges.txt")).map((i) -> i.substring(0, i.indexOf(" ") == -1 ? i.length() : i.indexOf(" ")).trim()).distinct()
                .collect(Collectors.partitioningBy((i) -> i.contains(" ")));
        //true 有空格,即多字符串
        //true 有空格,即多字符串
        //false 无空格,即一个字符串
        List<String> singleString = map.get(Boolean.FALSE);
        String s = singleString.stream().collect(Collectors.joining(" "));
        ByteArrayInterface src = new DefaultByteArray(s.getBytes());
        parser.parse(src, context);
        HashArray hashArray = context.getHashArray();
        int count = hashArray.getCount();
        int pos = 0;
        while (pos < count) {
            System.out.println(String.format("public final static int %s=%d;", singleString.get(pos), hashArray.getIntHash(pos)));
            ++pos;
        }
    }
}

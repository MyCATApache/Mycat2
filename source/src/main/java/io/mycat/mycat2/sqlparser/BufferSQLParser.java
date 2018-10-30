package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.byteArrayInterface.*;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dcl.DCLSQLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.function.Predicate;

import static io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil.debug;

/**
 * Created by Kaiz on 2017/2/6.
 * <p>
 * 2017/2/24
 * 表名规范遵循 ： https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
 * 暂时不支持字符型表名
 * https://github.com/mysql/mysql-workbench/blob/1972008552c725c4176fb83a35734cf2c1f6158c/library/parsers/grammars/MySQL.g
 * 2017/2/25
 * SQLBenchmark.DruidTest          thrpt   10   225009.114 ±  4904.006  ops/s
 * SQLBenchmark.NewSqQLParserTest  thrpt   10  3431639.918 ± 54679.339  ops/s
 * SQLBenchmark.SQLParserTest      thrpt   10  1297501.144 ± 56213.315  ops/s
 * 2017/2/26
 * 以 -- 开头的注释直接跳过
 * sql内字符串只保留字符串标识，第二阶段需要的话再根据pos和size进行解析
 * sql内数值只保留数值标识，第二阶段需要的话再根据pos和size进行解析
 * hash数组会保留两种内容，一种是标志位(<41)标识当前位置是什么东西，比如标点符号或者字符串或者数值等等，主要为第二阶段解析提供协助，尽量避免第二阶段需要重新遍历字串
 * 另一种是哈希值(>=41)，主要是sql关键字、表名、库名、别名，部分长度超过12个字符的有可能发生哈希值碰撞，这时可以先比较size是否一致，然后进行逐字符匹配
 * 计划用建立512大小的关键字哈希索引数组，最长碰撞为5，数组有效数量是379（ & 0x3fe >> 1）
 * 2017/3/8
 * 还可以进行如下优化：
 * 1. 部分变量可以替换成常量（是否需要预编译）
 * 2. 使用堆外unsafe数组
 * 3. SQLContext还需要优化成映射到hashArray的模式，也可以考虑只用一个数组，同时存储hashArray、charType、token和解析结果（估计也没人看得懂了）
 * 2017/3/31
 * NewSQLContext部分还需要进行如下优化
 * 1. 支持cache hint 例如 /*!mycat:cache-time=xxx auto-refresh=true access-count=5000...
 * 2. 支持每一句SQL都有独立的注解
 * 3. 支持包含注解的语句提取原始sql串
 */


public class BufferSQLParser {
    ByteArrayInterface sql;
    HashArray hashArray;// = new HashArray();
    Tokenizer2 tokenizer = new Tokenizer2();
    DefaultByteArray defaultByteArray = new DefaultByteArray();
    ByteBufferArray byteBufferArray = new ByteBufferArray();

    private static final Logger logger = LoggerFactory.getLogger(BufferSQLParser.class);

    int pickTableNames(int pos, final int arrayCount, BufferSQLContext context) {
        int type;
        long hash = hashArray.getHash(pos);
        if (hash != 0) {
            context.setTblName(pos);
            //context.setTblNameStart(hashArray.getPos(pos));// TODO: 2017/3/10 可以优化成一个接口
            //context.setTblNameSize(hashArray.getSize(pos));
            pos++;
            while (pos < arrayCount) {
                type = hashArray.getType(pos);
                if (type == Tokenizer2.DOT) {
                    ++pos;
                    context.pushSchemaName(pos);
                    //context.setTblNameStart(hashArray.getPos(pos));// TODO: 2017/3/10 可以优化成一个接口
                    //context.setTblNameSize(hashArray.getSize(pos));
                    ++pos;
                } else if (type == Tokenizer2.SEMICOLON) {
                    return pos;
                } else if (type == Tokenizer2.RIGHT_PARENTHESES || type == Tokenizer2.LEFT_PARENTHESES) {
                    return ++pos;
                } else if (type == Tokenizer2.COMMA) {
                    return pickTableNames(++pos, arrayCount, context);
                } else if ((type = hashArray.getIntHash(pos)) == IntTokenHash.AS) {
                    pos += 2;// TODO: 2017/3/10  二阶段解析需要别名，需要把别名存储下来
                } else if (TokenizerUtil.isAlias(pos, type, hashArray)) {
                    pos++;// TODO: 2017/3/10  二阶段解析需要别名，需要把别名存储下来
                } else
                    return pos;
            }
            return pos;
        } else {//判断 ,( 这样的情况
            return ++pos;
        }
    }


    int pickLimits(int pos, final int arrayCount, BufferSQLContext context) {
        int minus = 1;
        if (hashArray.getType(pos) == Tokenizer2.DIGITS) {
            context.setLimit();
            context.setLimitCount(TokenizerUtil.pickNumber(pos, hashArray, sql));
            if (++pos < arrayCount && hashArray.getType(pos) == Tokenizer2.COMMA) {
                context.pushLimitStart();
                if (++pos < arrayCount) {
                    if (hashArray.getType(pos) == Tokenizer2.MINUS) {
                        minus = -1;
                        ++pos;
                    }
                    if (hashArray.getType(pos) == Tokenizer2.DIGITS) {
                        //// TODO: 2017/3/11 需要完善处理数字部分逻辑
                        context.setLimitCount(TokenizerUtil.pickNumber(pos, hashArray, sql) * minus);
                    }
                }
            } else if (hashArray.getHash(pos) == TokenHash.OFFSET) {
                context.setLimitStart(TokenizerUtil.pickNumber(++pos, hashArray, sql));
            }
        }
        return pos;
    }

    int pickInsert(int pos, final int arrayCount, BufferSQLContext context) {
        int intHash;
        long hash;
        while (pos < arrayCount) {
            intHash = hashArray.getIntHash(pos);
            hash = hashArray.getHash(pos);
            if (intHash == IntTokenHash.INTO && hash == TokenHash.INTO) {
                return pickTableNames(++pos, arrayCount, context);
            } else if (intHash == IntTokenHash.DELAYED && hash == TokenHash.DELAYED) {
                pos++;
            } else if (intHash == IntTokenHash.LOW_PRIORITY && hash == TokenHash.LOW_PRIORITY) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.HIGH_PRIORITY && hash == TokenHash.HIGH_PRIORITY) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.IGNORE && hash == TokenHash.IGNORE) {
                pos++;
                continue;
            } else {
                return pickTableNames(pos, arrayCount, context);
            }

        }
        return pos;
    }

    int pickTableToken(int pos, final int arrayCount, BufferSQLContext context) {
        int intHash;
        long hash;
        while (pos < arrayCount) {
            intHash = hashArray.getIntHash(pos);
            hash = hashArray.getHash(pos);
            if (intHash == IntTokenHash.IF && hash == TokenHash.IF) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.NOT && hash == TokenHash.NOT) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.EXISTS && hash == TokenHash.EXISTS) {
                pos++;
                continue;
            } else {
                return pickTableNames(pos, arrayCount, context);
            }
        }
        return pos;
    }

    int pickUpdate(int pos, final int arrayCount, BufferSQLContext context) {
        int intHash;
        long hash;
        while (pos < arrayCount) {
            intHash = hashArray.getIntHash(pos);
            hash = hashArray.getHash(pos);
            if (intHash == IntTokenHash.LOW_PRIORITY && hash == TokenHash.LOW_PRIORITY) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.IGNORE && hash == TokenHash.IGNORE) {
                pos++;
                continue;
            } else {
                return pickTableNames(pos, arrayCount, context);
            }

        }
        return pos;
    }

    int pickAnnotation(int pos, final int arrayCount, BufferSQLContext context) {
        int intHash;
        long hash;
        while (pos < arrayCount) {
            intHash = hashArray.getIntHash(pos);
            hash = hashArray.getHash(pos);
            switch (intHash) {
                case IntTokenHash.ANNOTATION_END:
                    context.setRealSQLOffset(++pos);
                    return pos;
                case IntTokenHash.DATANODE:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_DATANODE);
                    if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                        context.setAnnotationValue(BufferSQLContext.ANNOTATION_DATANODE, hashArray.getHash(++pos));
                    }
                    break;
                case IntTokenHash.MERGE:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_MERGE);
                    MergeAnnotation annotation = context.getMergeAnnotation();
                    Predicate<Integer> isFinished = (i) -> {
                        switch (i) {
                            case IntTokenHash.DATANODE:
                            case IntTokenHash.GROUP_COLUMNS:
                            case IntTokenHash.MERGE_COLUMNS:
                            case IntTokenHash.HAVING:
                            case IntTokenHash.LIMIT_START:
                            case IntTokenHash.LIMIT_SIZE:
                            case IntTokenHash.ORDER:
                                return true;
                            default:
                                return false;
                        }
                    };
                    ++pos;
                    while (pos < arrayCount) {
                        intHash = hashArray.getIntHash(pos);
                        switch (intHash) {
                            case IntTokenHash.DATANODES: {
                                if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                                    while (pos < arrayCount) {
                                        annotation.addDataNode(++pos);
                                        if (isFinished.test(hashArray.getIntHash(pos + 1))) {
                                            ++pos;
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case IntTokenHash.GROUP_COLUMNS: {
                                if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                                    while (pos < arrayCount) {
                                        annotation.addGroupColumn(++pos);
                                        if (isFinished.test(hashArray.getIntHash(pos + 1))) {
                                            ++pos;
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case IntTokenHash.MERGE_COLUMNS: {
                                if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                                    while (pos < arrayCount) {
                                        ++pos;
                                        int colPos = pos;
                                        annotation.addMergeColumn(colPos, ++pos);
                                        if (isFinished.test(hashArray.getIntHash(pos + 1))) {
                                            ++pos;
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case IntTokenHash.HAVING: {
                                if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                                    annotation.setLeft(++pos);
                                    annotation.setOp(++pos);//@todo bug can not getType > <
                                    annotation.setRight(++pos);
                                    ++pos;
                                }
                                break;
                            }
                            case IntTokenHash.ORDER: {
                                if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                                    while (pos < arrayCount) {
                                        ++pos;
                                        int colPos = pos;
                                        int order = hashArray.getIntHash(++pos);
                                        MergeAnnotation.OrderType orderType = order == IntTokenHash.DESC ?
                                                MergeAnnotation.OrderType.DESC : MergeAnnotation.OrderType.ASC;
                                        annotation.addOrderColumn(colPos, orderType);
                                        if (isFinished.test(hashArray.getIntHash(pos + 1))) {
                                            ++pos;
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            case IntTokenHash.LIMIT_START: {
                                if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                                    ///@todo ref pickLimits,change pickNumber retuen int type to long type
                                    //ref pickLimits
                                    annotation.setLimitStart(TokenizerUtil.pickNumber(++pos, hashArray, sql) * 1);
                                    ++pos;
                                }
                                break;
                            }
                            case IntTokenHash.LIMIT_SIZE: {
                                if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                                    ///@todo  ref pickLimits,change pickNumber retuen int type to long type
                                    annotation.setLimitSize(TokenizerUtil.pickNumber(++pos, hashArray, sql) * 1);
                                    ++pos;
                                }
                                break;
                            }
                            default: {
                                ++pos;
                                context.setRealSQLOffset(pos);
//                                System.out.println(this.sql.getStringByHashArray(pos, this.hashArray)+":"+intHash);
                                return pos;
                            }
                        }
                    }
                    break;
                case IntTokenHash.SCHEMA:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_SCHEMA);
                    if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                        context.setAnnotationValue(BufferSQLContext.ANNOTATION_SCHEMA, hashArray.getHash(++pos));
                    }
                    break;
                case IntTokenHash.SQL:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_SQL);
                    if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {

                    }
                    break;
                case IntTokenHash.CATLET:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_CATLET);
                    if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                        int start = hashArray.getPos(++pos);
                        int length = hashArray.getSize(pos);
                        intHash = hashArray.getIntHash(++pos);
                        while (pos < arrayCount && intHash != IntTokenHash.ANNOTATION_END) {
                            length += hashArray.getSize(pos) + 1;//+1是因为前面的 . 没有被收入HashArray
                            intHash = hashArray.getIntHash(++pos);
                        }
                        context.setCatletName(start, length);
                        context.setAnnotationStringValue(BufferSQLContext.ANNOTATION_CATLET, context.getCatletName());

                    }
                    break;
                case IntTokenHash.DB_TYPE:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_DB_TYPE);
                    if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                        context.setAnnotationValue(BufferSQLContext.ANNOTATION_DB_TYPE, hashArray.getHash(++pos));
                        ++pos;
                    }
                    break;
                case IntTokenHash.ACCESS_COUNT:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_SQL_CACHE);
                    if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                        context.setAnnotationValue(BufferSQLContext.ANNOTATION_ACCESS_COUNT, TokenizerUtil.pickNumber(++pos, hashArray, sql));
                        ++pos;
                    }
                    break;
                case IntTokenHash.AUTO_REFRESH:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_SQL_CACHE);
                    if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                        context.setAnnotationValue(BufferSQLContext.ANNOTATION_AUTO_REFRESH, hashArray.getHash(++pos));
                        ++pos;
                    }
                    break;
                case IntTokenHash.CACHE_TIME:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_SQL_CACHE);
                    if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                        context.setAnnotationValue(BufferSQLContext.ANNOTATION_CACHE_TIME, TokenizerUtil.pickNumber(++pos, hashArray, sql));
                        ++pos;
                    }
                    break;
                case IntTokenHash.CACHE_RESULT:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_SQL_CACHE);
                    ++pos;
                    break;
                case IntTokenHash.BALANCE:
                    if (hashArray.getHash(pos) == TokenHash.BALANCE) {
                        context.setAnnotationType(BufferSQLContext.ANNOTATION_BALANCE);
                        if (hashArray.getHash(++pos) == TokenHash.TYPE) {
                            if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                                context.setAnnotationValue(BufferSQLContext.ANNOTATION_BALANCE, hashArray.getHash(++pos));
                                ++pos;
                            }
                        }
                    }
                    break;
                case IntTokenHash.REPLICA:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_REPLICA_NAME);
                    if (hashArray.getHash(++pos) == TokenHash.NAME) {
                        if (hashArray.getType(++pos) == Tokenizer2.EQUAL) {
                            context.setAnnotationValue(BufferSQLContext.ANNOTATION_REPLICA_NAME, hashArray.getHash(++pos));
                            ++pos;
                        }
                    }
                    break;
                default:
                    ++pos;
            }
        }
        return pos;
    }

    int pickSchemaToken(int pos, BufferSQLContext context) {
        context.setTblName(pos);
        context.pushSchemaName(pos);
        return ++pos;
    }


    int pickLoad(int pos, final int arrayCount, BufferSQLContext context) {
        int intHash;
        long hash;
        context.setSQLType(BufferSQLContext.LOAD_SQL);
        ++pos;//skip DATA / XML token
        while (pos < arrayCount) {
            intHash = hashArray.getIntHash(pos);
            hash = hashArray.getHash(pos);
            switch (intHash) {
                case IntTokenHash.XML:
                    pos++;
                    break;
                case IntTokenHash.DATA:
                    pos++;
                    break;
                case IntTokenHash.LOW_PRIORITY:
                    pos++;
                    break;
                case IntTokenHash.CONCURRENT:
                    pos++;
                    break;
                case IntTokenHash.LOCAL:
                    pos++;
                    break;
                case IntTokenHash.INFILE:
                    pos += 2;
                    break;
                case IntTokenHash.REPLACE:
                    pos++;
                    break;
                case IntTokenHash.IGNORE:
                    pos++;
                    break;
                case IntTokenHash.INTO:
                    return pickTableNames(pos + 2, arrayCount, context);
                default:
                    pos++;
                    break;

            }
        }
        return pos;
    }


    /**
     * 用于进行第一遍处理，处理sql类型以及提取表名
     **/
    public void firstParse(BufferSQLContext context) {
        final int arrayCount = hashArray.getCount();
        int pos = 0;
        while (pos < arrayCount) {
            switch (hashArray.getIntHash(pos)) {
                case IntTokenHash.FROM:
                    if (hashArray.getHash(pos) == TokenHash.FROM) {
                        pos = pickTableNames(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.INTO: {
                    byte type = context.getSQLType();
                    if (context.getCurSQLType() == BufferSQLContext.SELECT_SQL) {
                        context.setSQLType(BufferSQLContext.SELECT_INTO_SQL);
                    }
                    if (hashArray.getHash(pos) == TokenHash.INTO) {
                        pos = pickTableNames(++pos, arrayCount, context);
                    }
                    break;
                }
                case IntTokenHash.TABLE:
                    if (hashArray.getHash(pos) == TokenHash.TABLE) {
                        pos = pickTableToken(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.JOIN:
                    if (hashArray.getHash(pos) == TokenHash.JOIN) {
                        pos = pickTableNames(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.UPDATE:
                    if (hashArray.getHash(pos) == TokenHash.UPDATE) {
                        context.setSQLType(BufferSQLContext.UPDATE_SQL);
                        pos = pickUpdate(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.USE:
                    if (hashArray.getHash(pos) == TokenHash.USE) {
                        context.setSQLType(BufferSQLContext.USE_SQL);
                        pos = pickSchemaToken(++pos, context);
                    }
                    break;
                case IntTokenHash.DELETE:
                    if (hashArray.getHash(pos) == TokenHash.DELETE) {
                        context.setSQLType(BufferSQLContext.DELETE_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.DROP:
                    if (hashArray.getHash(pos) == TokenHash.DROP) {
                        context.setSQLType(BufferSQLContext.DROP_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.SELECT:
                    if (hashArray.getHash(pos) == TokenHash.SELECT) {
                        context.setSQLType(BufferSQLContext.SELECT_SQL);
                        pos = SelectItemsParser.pickItemList(++pos, arrayCount, hashArray, context);
                    }
                    break;
                case IntTokenHash.SHOW:
                    if (hashArray.getHash(pos) == TokenHash.SHOW) {
                        context.setSQLType(BufferSQLContext.SHOW_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.DATABASES:
                    if (hashArray.getHash(pos) == TokenHash.DATABASES) {
                        context.setShowSQLType(BufferSQLContext.SHOW_DB_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.TABLES:
                    if (hashArray.getHash(pos) == TokenHash.TABLES) {
                        context.setShowSQLType(BufferSQLContext.SHOW_TB_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.INSERT:
                    if (hashArray.getHash(pos) == TokenHash.INSERT) {
                        context.setSQLType(BufferSQLContext.INSERT_SQL);
                        pos = pickInsert(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.LIMIT:
                    if (hashArray.getHash(pos) == TokenHash.LIMIT) {
                        pos = pickLimits(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.TRUNCATE:
                    if (hashArray.getHash(pos) == TokenHash.TRUNCATE) {
                        context.setSQLType(BufferSQLContext.TRUNCATE_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.ALTER:
                    if (hashArray.getHash(pos) == TokenHash.ALTER) {
                        context.setSQLType(BufferSQLContext.ALTER_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.CREATE:
                    if (hashArray.getHash(pos) == TokenHash.CREATE) {
                        context.setSQLType(BufferSQLContext.CREATE_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.REPLACE:
                    if (hashArray.getHash(pos) == TokenHash.REPLACE) {
                        context.setSQLType(BufferSQLContext.REPLACE_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.SET:
                    if (hashArray.getHash(pos) == TokenHash.SET) {
                        pos = TCLSQLParser.pickSetAutocommitAndSetTransaction(++pos, arrayCount, context, hashArray, sql);
                    }
                    break;
                case IntTokenHash.COMMIT:
                    if (hashArray.getHash(pos) == TokenHash.COMMIT) {
                        context.setSQLType(BufferSQLContext.COMMIT_SQL);
                        pos++;
                        TokenizerUtil.debug(() -> "COMMIT");
                        pos = TCLSQLParser.pickCommitRollback(pos, arrayCount, context, hashArray);
                    }
                    break;
                case IntTokenHash.START:
                    if (hashArray.getHash(pos) == TokenHash.START) {
                        context.setSQLType(BufferSQLContext.START_SQL);
                        pos++;
                        TokenizerUtil.debug(() -> "START");
                        if (hashArray.getHash(pos) == TokenHash.TRANSACTION) {
                            pos = TCLSQLParser.pickStartTransaction(++pos, arrayCount, context, hashArray);
                        }
                    }
                    break;
                case IntTokenHash.BEGIN:
                    if (hashArray.getHash(pos) == TokenHash.BEGIN) {
                        context.setSQLType(BufferSQLContext.BEGIN_SQL);
                        pos++;
                        TokenizerUtil.debug(() -> "BEGIN");
                        if (hashArray.getHash(pos) == TokenHash.WORK) {
                            TokenizerUtil.debug(() -> "WORK");
                            //todo WORK
                            pos++;
                        }
                    }
                    break;
                case IntTokenHash.SAVEPOINT:
                    debug(pos, context);
                    if (hashArray.getHash(pos) == TokenHash.SAVEPOINT) {
                        context.setSQLType(BufferSQLContext.SAVEPOINT_SQL);
                        pos++;
                        //todo 记录    SAVEPOINT identifier
                        TokenizerUtil.debug(pos, tokenizer, hashArray);
                        pos++;
                    }
                    break;
                case IntTokenHash.KILL:
                    if (hashArray.getHash(pos) == TokenHash.KILL) {

                        if (hashArray.getIntHash(++pos) == IntTokenHash.QUERY && hashArray.getHash(pos) == TokenHash.QUERY) {
                            context.setSQLType(BufferSQLContext.KILL_QUERY_SQL);
                        } else
                            context.setSQLType(BufferSQLContext.KILL_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.CALL:
                    if (hashArray.getHash(pos) == TokenHash.CALL) {
                        context.setSQLType(BufferSQLContext.CALL_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.DESC:
                    if (context.getCurSQLType() != 0) {
                        pos++;
                        break;
                    }
                case IntTokenHash.DESCRIBE:
                    long hashValue;
                    if (((hashValue = hashArray.getHash(pos)) == TokenHash.DESC) ||
                            hashValue == TokenHash.DESCRIBE) {
                        context.setSQLType(BufferSQLContext.DESCRIBE_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.LOAD:
                    if (hashArray.getHash(pos) == TokenHash.LOAD) {
                        pos = pickLoad(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.HELP:
                    if (hashArray.getHash(pos) == TokenHash.HELP) {
                        context.setSQLType(BufferSQLContext.HELP_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.ROLLBACK:
                    if (hashArray.getHash(pos) == TokenHash.ROLLBACK) {
                        context.setSQLType(BufferSQLContext.ROLLBACK_SQL);
                        pos++;
                        TokenizerUtil.debug(() -> "ROLLBACK");
                        pos = TCLSQLParser.pickCommitRollback(pos, arrayCount, context, hashArray);
                    }
                    break;
                case IntTokenHash.ANNOTATION_BALANCE:
                    context.setAnnotationType(BufferSQLContext.ANNOTATION_BALANCE);
                    pos++;
                    break;
                case IntTokenHash.ANNOTATION_START:
                    pos = pickAnnotation(++pos, arrayCount, context);
                    break;
                case IntTokenHash.SQL_DELIMETER:
                    if (hashArray.getHash(pos) == 0) {
                        context.setSQLFinished(++pos);
                    } else {
                        pos++;
                    }
                    break;
                case IntTokenHash.FOR:
                    int next = pos + 1;
                    if (context.getCurSQLType() == BufferSQLContext.SELECT_SQL) {
                        if (hashArray.getIntHash(next) == IntTokenHash.UPDATE && hashArray.getHash(next) == TokenHash.UPDATE) {
                            context.setSQLType(BufferSQLContext.SELECT_FOR_UPDATE_SQL);
                        }
                    }
                case IntTokenHash.RELEASE: {
                    TokenizerUtil.debug(pos, context);
                    ++pos;
                    if (hashArray.getHash(pos) == TokenHash.SAVEPOINT) {
                        TokenizerUtil.debug(pos, context);
                        ++pos;
                        //todo 记录 RELEASE SAVEPOINT identifier
                        TokenizerUtil.debug(pos, context);
                        ++pos;
                    }
                    break;
                }
                case IntTokenHash.LOCK: {
                    debug(pos, context);
                    ++pos;
                    if (hashArray.getHash(pos) == TokenHash.TABLES) {
                        debug(pos, context);
                        pos = TCLSQLParser.pickLockTables(++pos, arrayCount, context, hashArray);
                    }
                    break;
                }
                case IntTokenHash.UNLOCK: {
                    ++pos;
                    if (hashArray.getHash(pos) == TokenHash.TABLES) {
                        //todo 记录SQL_TYPE
                        ++pos;
                    }
                    break;
                }
                case IntTokenHash.XA: {
                    debug(pos, context);
                    pos = TCLSQLParser.pickXATransaction(++pos, arrayCount, context, hashArray);
                    break;
                }
                case IntTokenHash.GRANT: {
                    TokenizerUtil.debug(pos, context);
                    pos = DCLSQLParser.pickGrant(++pos, arrayCount, context, hashArray, sql);
                    break;
                }
                case IntTokenHash.REVOKE: {
                    TokenizerUtil.debug(pos, context);
                    pos = DCLSQLParser.pickRevoke(++pos, arrayCount, context, hashArray, sql);
                    break;
                }
                case IntTokenHash.MYCAT: {
                    if (hashArray.getHash(pos) == TokenHash.MYCAT) {
                        context.setSQLType(BufferSQLContext.MYCAT_SQL);
                        pos++;
                    }
                    break;
                }
                case IntTokenHash.SHUTDOWN:
                    if (hashArray.getHash(pos) == TokenHash.SHUTDOWN) {
                        context.setSQLType(BufferSQLContext.SHUTDOWN_SQL);
                        pos++;
                    }
                    break;
                default:
                    //  debugError(pos, context);
                    pos++;
                    break;
            }
        }
        context.setSQLFinished(pos);//为了确保最后一个没有分号的sql也能正确处理
    }

    /*
     * 计划用于第二遍解析，处理分片表分片条件
     */
    public void secondParse() {

    }

    public void setOffset(int offset) {

    }

    public static void main(String[] args) {
        BufferSQLParser parser = new BufferSQLParser();
        BufferSQLContext context = new BufferSQLContext();
        //parser.init();
//        byte[] defaultByteArray = "SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));".getBytes(StandardCharsets.UTF_8);//20个token
//        byte[] defaultByteArray = "INSERT `mycatSchema`.`tbl_A` (`name`) VALUES ('kaiz');".getBytes(StandardCharsets.UTF_8);
//        byte[] defaultByteArray = ("select * from tbl_A, -- 单行注释\n" +
//                "tbl_B b, #另一种单行注释\n" +
//                "/*\n" +  //69
//                "tbl_C\n" + //79
//                "*/ tbl_D d;").getBytes(StandardCharsets.UTF_8);
//        byte[] defaultByteArray = sql3.getBytes(StandardCharsets.UTF_8);
//        byte[] defaultByteArray = "SELECT * FROM table LIMIT 95,-1".getBytes(StandardCharsets.UTF_8);
//        byte[] defaultByteArray = "/*balance*/select * from tbl_A where id=1;".getBytes(StandardCharsets.UTF_8);
//        byte[] defaultByteArray = "/*!MyCAT:DB_Type=Master*/select * from tbl_A where id=1;".getBytes(StandardCharsets.UTF_8);
//        byte[] defaultByteArray = "insert tbl_A(id, val) values(1, 2);\ninsert tbl_B(id, val) values(2, 2);\nSELECT id, val FROM tbl_S where id=19;\n".getBytes(StandardCharsets.UTF_8);

        ByteArrayInterface src = new DefaultByteArray("/* mycat:balance*/select * into tbl_B from tbl_A;".getBytes());
//        ByteArrayInterface src = new DefaultByteArray("select VERSION(), USER(), id from tbl_A;".getBytes());
//        ByteArrayInterface src = new DefaultByteArray("select * into tbl_B from tbl_A;".getBytes());
//        long min = 0;
//        for (int i = 0; i < 50; i++) {
//            System.out.print("Loop " + i + " : ");
//            long cur = RunBench(defaultByteArray, parser);//不加分析应该可以进2.6秒
//            System.out.println(cur);
//            if (cur < min || min == 0) {
//                min = cur;
//            }
//        }
//        System.out.print("min time : " + min);
        parser.parse(src, context);
        System.out.println(context.getSQLCount());
        System.out.println(context.getSelectItem(0));
        System.out.println(context.getSelectItem(1));
        //IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        //System.out.print("token count : "+parser.hashArray.getCount());
    }


    public void parse(ByteArrayInterface src, BufferSQLContext context) {
        sql = src;
        hashArray = context.getHashArray();
        hashArray.init();
        context.setCurBuffer(src);
        tokenizer.tokenize(src, hashArray);
        firstParse(context);
    }

    public void parse(byte[] src, BufferSQLContext context) {
        this.defaultByteArray.setSrc(src);
        sql = this.defaultByteArray;
        hashArray = context.getHashArray();
        hashArray.init();
        context.setCurBuffer(sql);
        tokenizer.tokenize(sql, hashArray);
        firstParse(context);
    }

//    static long RunBench(byte[] defaultByteArray, NewSQLParser parser) {
//        int count = 0;
//        long start = System.currentTimeMillis();
//        do {
//            parser.tokenize(defaultByteArray);
//        } while (count++ < 10_000_000);
//        return System.currentTimeMillis() - start;
//    }

    public void parse(ByteBuffer src, int offset, int length, BufferSQLContext context) {
        this.byteBufferArray.setSrc(src);
        this.byteBufferArray.setOffset(offset);
        this.byteBufferArray.setLength(length);
        if (logger.isDebugEnabled()) {
            logger.debug("Recieved SQL : " + this.byteBufferArray.getString(offset, length));
        }
        sql = this.byteBufferArray;
        hashArray = context.getHashArray();
        hashArray.init();
        context.setCurBuffer(sql);
        tokenizer.tokenize(sql, hashArray);
        firstParse(context);
        //System.out.println("getRealSQL : "+context.getRealSQL(0)+" #limit count : "+context.getLimitCount());
    }

    static String sql3 = "SELECT  'product' as 'P_TYPE' ,\n" +
            "\t \t\tp.XINSHOUBIAO,\n" +
            "\t\t\t0 AS TRANSFER_ID,\n" +
            "\t\t\tp.PRODUCT_ID ,\n" +
            "\t\t\tp.PRODUCT_NAME,\n" +
            "\t\t\tp.PRODUCT_CODE,\n" +
            "\t\t\tROUND(p.APPLY_INTEREST,4) AS APPLY_INTEREST,\n" +
            "\t\t\tp.BORROW_AMOUNT,\n" +
            "\t\t\tCASE  WHEN p.FangKuanDate IS NULL THEN\n" +
            "\t\t\t\tDATEDIFF(\n" +
            "\t\t\t\t\tp.APPLY_ENDDATE,\n" +
            "\t\t\t\t\tDATE_ADD(\n" +
            "\t\t\t\t\t\tp.RAISE_END_TIME,\n" +
            "\t\t\t\t\t\tINTERVAL 1 DAY\n" +
            "\t\t\t\t\t)\n" +
            "\t\t\t\t) +1\n" +
            "\t\t\tELSE\n" +
            "\t\t\t\tDATEDIFF(\n" +
            "\t\t\t\t\tp.APPLY_ENDDATE,\n" +
            "\t\t\t\t\tDATE_ADD(p.FangKuanDate, INTERVAL 1 DAY)\n" +
            "\t\t\t\t) +1\n" +
            "\t\t\tEND\t AS APPLY_ENDDAY,\n" +
            "\t\t\t'' AS APPLY_ENDDATE,\n" +
            "\t\t\tp.BORROW_ENDDAY,\n" +
            "\t\t\t0 AS TRANSFER_HONGLI,\n" +
            "\t\t\tp.BORROW_MONTH_TYPE,\n" +
            "\t\t\tIFNULL(p.INVEST_SCHEDUL,0) AS INVEST_SCHEDUL,\n" +
            "\t\t\tDATE_FORMAT(\n" +
            "\t\t\t\tp.Product_pub_date,\n" +
            "\t\t\t\t'%Y-%m-%d %H:%i:%s'\n" +
            "\t\t\t) AS Product_pub_date,\n" +
            " \t\t\td.DIZHIYA_TYPE_NAME,\n" +
            "\t\t\tp.PRODUCT_TYPE_ID,\n" +
            "\t\t\tp.PRODUCT_STATE,\n" +
            "\t\t\tp.PRODUCT_LIMIT_TYPE_ID,\n" +
            "\t\t\tp.PAYBACK_TYPE,\n" +
            "\t\t\tp.TARGET_TYPE_ID,\n" +
            "\t\t\tp.COUPONS_TYPE,\n" +
            "      0 AS TRANSFER_TIME,\n" +
            "      P.MANBIAODATE AS  MANBIAODATE\n" +
            "\t\tFROM\n" +
            "\t\t\tTProduct p\n" +
            "\t\tJOIN TJieKuanApply j ON p.APPLY_NO = j.APPLY_NO\n" +
            "\t\tJOIN TDiZhiYaType d ON d.DIZHIYA_TYPE = j.DIZHIYA_TYPE\n" +
            "\t\tJOIN (\n" +
            "\t\t\tSELECT\n" +
            "\t\t\n" +
            "\t\t\t\tPRODUCT_ID,\n" +
            "\t\t\t\tCASE\n" +
            "\t\t\tWHEN APPLY_ENDDATE IS NOT NULL THEN\n" +
            "\t\t\t\tCASE  WHEN FangKuanDate IS NULL THEN\n" +
            "\t\t\t\t\tDATEDIFF(\n" +
            "\t\t\t\t\t\tAPPLY_ENDDATE,\n" +
            "\t\t\t\t\t\tDATE_ADD(\n" +
            "\t\t\t\t\t\t\tRAISE_END_TIME,\n" +
            "\t\t\t\t\t\t\tINTERVAL 1 DAY\n" +
            "\t\t\t\t\t\t)\n" +
            "\t\t\t\t\t) +1\n" +
            "\t\t\t\tELSE\n" +
            "\t\t\t\t\tDATEDIFF(\n" +
            "\t\t\t\t\t\tAPPLY_ENDDATE,\n" +
            "\t\t\t\t\t\tDATE_ADD(FangKuanDate, INTERVAL 1 DAY)\n" +
            "\t\t\t\t\t) +1\n" +
            "\t\t\t\tEND\n" +
            "\t\t\tWHEN BORROW_MONTH_TYPE = 1 THEN\n" +
            "\t\t\t\tBORROW_ENDDAY\n" +
            "\t\t\tWHEN BORROW_MONTH_TYPE = 2 THEN\n" +
            "\t\t\t\tBORROW_ENDDAY * 30\n" +
            "\t\t\tELSE\n" +
            "\t\t\t\tBORROW_ENDDAY\n" +
            "\t\t\tEND AS DAYS\n" +
            "\t\t\tFROM\n" +
            "\t\t\t\tTProduct\n" +
            "\t\t\t) m ON p.PRODUCT_ID = m.PRODUCT_ID\n" +
            "\t\tWHERE\n" +
            "\t\t     1 = 1\n" +
            "\t\t     AND p.PRODUCT_STATE IN(4,5,8) \n" +
            "\t\t     AND (p.PRODUCT_ID) NOT IN (\n" +
            "\t\t\t SELECT PRODUCT_ID FROM TProduct WHERE PRODUCT_STATE = 4 ";
}

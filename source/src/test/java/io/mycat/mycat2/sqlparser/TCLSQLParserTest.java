package io.mycat.mycat2.sqlparser;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by cjw on 2017/1/22.
 */
public class TCLSQLParserTest extends TestCase {
    //    SQLParser parser;
    BufferSQLParser parser;
    BufferSQLContext context;
    private static final Logger LOGGER = LoggerFactory.getLogger(TCLSQLParserTest.class);

    private void test(String t) {
        LOGGER.info(t);
        byte[] bytes = t.getBytes();
        parser.parse(bytes, context);
    }

    @Before
    protected void setUp() throws Exception {
        parser = new BufferSQLParser();
        context = new BufferSQLContext();
        //parser.init();
        MatchMethodGenerator.initShrinkCharTbl();
    }

    /**START TRANSACTION, COMMIT, and ROLLBACK Syntax
     *
     START TRANSACTION
     [transaction_characteristic [, transaction_characteristic] ...]

     transaction_characteristic:
     WITH CONSISTENT SNAPSHOT
     | READ WRITE
     | READ ONLY

     BEGIN [WORK]
     COMMIT [WORK] [AND [NO] CHAIN] [[NO] RELEASE]
     ROLLBACK [WORK] [AND [NO] CHAIN] [[NO] RELEASE]
     SET autocommit = {0 | 1}
     *
     *
     *
     */

    @Test
    public void testStartTransactionWithConsistentSnapshot() throws Exception {
        String t = "START TRANSACTION WITH CONSISTENT SNAPSHOT;";
        test(t);
    }

    @Test
    public void testStartTransaction() throws Exception {
        String t = "START TRANSACTION;";
        test(t);
    }

    @Test
    public void testStartTransactionReadWrite() throws Exception {
        String t = "START TRANSACTION READ WRITE;";
        test(t);
    }

    @Test
    public void testStartTransactionReadOnly() throws Exception {
        String t = "START TRANSACTION READ ONLY;";
        test(t);
    }

    @Test
    public void testStartTransactionTwoParameters() throws Exception {
        String t = "START TRANSACTION READ ONLY,READ WRITE;";
        test(t);
    }

    @Test
    public void testStartTransactionThreeParameters() throws Exception {
        String t = "START TRANSACTION WITH CONSISTENT SNAPSHOT,READ WRITE,READ ONLY;";
        test(t);
    }

    @Test
    public void testSetAutocommit() throws Exception {
        String t = "SET AUTOCOMMIT = 1;";
        test(t);
    }

    @Test
    public void testBegin() throws Exception {
        String t = "BEGIN;";
        test(t);
    }

    @Test
    public void testBeginWork() throws Exception {
        String t = "BEGIN WORK;";
        test(t);
    }

    @Test
    public void testCommit() throws Exception {
        String t = "COMMIT WORK AND NO CHAIN NO RELEASE";
        test(t);
    }

    @Test
    public void testCommit1() throws Exception {
        String t = "COMMIT WORK AND NO CHAIN  RELEASE;";
        test(t);
        ;
    }

    @Test
    public void testCommit2() throws Exception {
        String t = "COMMIT WORK AND  CHAIN  RELEASE;";
        test(t);
    }

    @Test
    public void testCommit3() throws Exception {
        String t = "COMMIT WORK AND CHAIN;";
        test(t);
    }

    @Test
    public void testCommit4() throws Exception {
        String t = "COMMIT WORK;";
        test(t);
    }

    @Test
    public void testCommit5() throws Exception {
        String t = "COMMIT;";
        test(t);
    }

    @Test
    public void testCommit6() throws Exception {
        String t = "COMMIT NO RELEASE;";
        test(t);
    }

    @Test
    public void testCommit7() throws Exception {
        String t = "COMMIT RELEASE;";
        test(t);
    }

    /**
     * for testCommit3
     **/
    @Test
    public void testCommit8() throws Exception {
        String t = "COMMIT WORK AND NO CHAIN;";
        test(t);
    }

    @Test
    public void testRollback() throws Exception {
        String t = "ROLLBACK WORK AND NO CHAIN NO RELEASE";
        test(t);
    }
    /**
     * SAVEPOINT, ROLLBACK TO SAVEPOINT, and RELEASE SAVEPOINT Syntax

     SAVEPOINT identifier
     ROLLBACK [WORK] TO [SAVEPOINT] identifier
     RELEASE SAVEPOINT identifier

     *上面的测试已经完成
    /**
     * 已完成
     * @throws Exception
     */
    @Test
    public void testSavepoint() throws Exception {
        String t = " SAVEPOINT identifier;";
        test(t);
    }

    /**
     *已完成
     * @throws Exception
     */
    @Test
    public void testRollbackToSavePoint() throws Exception {
        String t = "ROLLBACK WORK TO SAVEPOINT identifier;";
        test(t);
    }

    /**
     * 已完成
     * @throws Exception
     */
    @Test
    public void testRollbackToSavePoint2() throws Exception {
        String t = "ROLLBACK  TO SAVEPOINT identifier;";
        test(t);
    }

    /**
     * 已完成
     * @throws Exception
     */
    @Test
    public void testRollbackToSavePoint3() throws Exception {
        String t = "ROLLBACK WORK TO  identifier;";
        test(t);
    }

    /**
     * 已完成
     * @throws Exception
     */
    @Test
    public void testRollbackToSavePoint4() throws Exception {
        String t = "ROLLBACK  TO  identifier;";
        test(t);
    }
    @Test
    public void testReleaseSavepoint() throws Exception {
        String t = "RELEASE SAVEPOINT identifier;";
        test(t);
    }
    /**
     * LOCK TABLES and UNLOCK TABLES Syntax
     *
     LOCK TABLES
     tbl_name [[AS] alias] lock_type
     [, tbl_name [[AS] alias] lock_type] ...

     lock_type:
     READ [LOCAL]
     | [LOW_PRIORITY] WRITE

     UNLOCK TABLES
     *
     */
    @Test
    public void testLocktables() throws Exception {
        String t = "LOCK TABLES user AS u READ LOCAL,book AS b LOW_PRIORITY WRITE,bag READ,car WRITE;";
        test(t);;
    }
    @Test
    public void testLocktables2() throws Exception {
        String t = "LOCK TABLES car WRITE;";
        test(t);
    }
    @Test
    public void testLocktables3() throws Exception {
        String t = "LOCK TABLES bag READ;";
        test(t);
    }
    @Test
    public void testUnLock() throws Exception {
        String t = "UNLOCK TABLES;";
        test(t);
    }
    /**
     * SET TRANSACTION Syntax
     *
     SET [GLOBAL | SESSION] TRANSACTION
     transaction_characteristic [, transaction_characteristic] ...

     transaction_characteristic:
     ISOLATION LEVEL level
     | READ WRITE
     | READ ONLY

     level:
     REPEATABLE READ
     | READ COMMITTED
     | READ UNCOMMITTED
     | SERIALIZABLE
     */
    @Test
    public void testSetTransaction() throws Exception {
        String t = "SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ," +
                "ISOLATION LEVEL READ COMMITTED," +
                "ISOLATION LEVEL READ UNCOMMITTED," +
                "ISOLATION LEVEL SERIALIZABLE," +
                "READ WRITE," +
                "READ ONLY;";
        test(t);
    }
    @Test
    public void testSetTransaction2() throws Exception {
        String t = "SET GLOBAL TRANSACTION " +
                "READ WRITE," +
                "ISOLATION LEVEL SERIALIZABLE," +
                "READ ONLY;";
        test(t);
    }
    /**
     *  XA Transaction SQL Syntax
     *
     XA {START|BEGIN} xid [JOIN|RESUME]

     XA END xid [SUSPEND [FOR MIGRATE]]

     XA PREPARE xid

     XA COMMIT xid [ONE PHASE]

     XA ROLLBACK xid

     XA RECOVER [CONVERT XID]

     xid: gtrid [, bqual [, formatID ]]

     gtrid是一个全局事务标识符，bqual是一个分支限定符，formatID是一个数字，用于标识gtrid和 bqual值使用的格式 。如语法所示，bqual并且 formatID是可选的。 如果没有给出默认 bqual值''。formatID 如果没有给出，默认值为1。

     gtrid并且 bqual必须是字符串文字，每个最多64个字节（不是字符）长。 gtrid并且 bqual可以以多种方式指定。您可以使用引用的string（'ab'），hex string（X'6162'，0x6162）或位值（）。 b'nnnn'

     formatID 是一个无符号整数。

     该gtrid和 bqual值以字节为MySQL服务器的底层XA支持例程解释。但是，当解析包含XA语句的SQL语句时，服务器可以使用某些特定的字符集。要安全，写gtrid和 bqual十六进制字符串。
     */
    final static String xid="100,'0x01','0x02'";
    @Test
    public void testXASTART() throws Exception {
        String t = "XA START "+xid;
        test(t);
    }
    @Test
    public void testXASTART2() throws Exception {
        String t = "XA START "+xid+" JOIN";
        test(t);
    }
    @Test
    public void testXASTART3() throws Exception {
        String t = "XA START "+xid+" RESUME";
        test(t);
    }
    @Test
    public void testXASTART4() throws Exception {
        String t = "XA BEGIN "+xid+" RESUME";
        test(t);
    }
    @Test
    public void testXAEND() throws Exception {
        String t = "XA END "+xid+" SUSPEND FOR MIGRATE;";
        test(t);
    }
    @Test
    public void testXAEND1() throws Exception {
        String t = "XA END "+xid+" SUSPEND ;";
        test(t);
    }

    /**
     *     XA END xid [SUSPEND [FOR MIGRATE]]
     * @throws Exception
     */
    @Test
    public void testXAEND2() throws Exception {
        String t = "XA END "+xid+" SUSPEND ;";
        test(t);
    }
    @Test
    public void testXAEND3() throws Exception {
        String t = "XA END "+xid+";";
        test(t);
    }

    /**
     * XA PREPARE xid
     * @throws Exception
     */
    @Test
    public void testXAPREPARE() throws Exception {
        String t = "XA PREPARE "+xid+";";
        test(t);
    }

    /**
     * XA COMMIT xid [ONE PHASE]
     * @throws Exception
     */
    @Test
    public void testXACOMMIT() throws Exception {
        String t = "XA COMMIT "+xid+" ONE PHASE;";
        test(t);
    }
    @Test
    public void testXACOMMIT2() throws Exception {
        String t = "XA COMMIT "+xid+" ;";
        test(t);
    }

    /**
     * XA ROLLBACK xid
     * @throws Exception
     */
    @Test
    public void testXAROLLBACK() throws Exception {
        String t = "XA ROLLBACK "+xid+";";
        test(t);
    }

    /**
     *
     XA RECOVER [CONVERT XID]
     * @throws Exception
     */
    @Test
    public void testXARECOVER() throws Exception {
        String t = "XA RECOVER CONVERT XID";
        test(t);
    }
    @Test
    public void testXARECOVER1() throws Exception {
        String t = "XA RECOVER;";
        test(t);
    }

}

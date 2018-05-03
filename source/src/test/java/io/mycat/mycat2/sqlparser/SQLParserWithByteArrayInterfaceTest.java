package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.SQLParseUtils.Tokenizer;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteBufferArray;
import io.mycat.mycat2.sqlparser.byteArrayInterface.Tokenizer2;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

/**
 * Created by Kaiz on 2017/1/22.
 */
public class SQLParserWithByteArrayInterfaceTest extends TestCase {
    final static BiConsumer<Object[], Object[]> assertEquals = (x, y) -> assertEquals(Arrays.toString(x), Arrays.toString(y));
    BufferSQLContext context;
    //    SQLParser parser;
    BufferSQLParser parser;

    @Before
    protected void setUp() {
        parser = new BufferSQLParser();
        context = new BufferSQLContext();
        //parser.init();
        MatchMethodGenerator.initShrinkCharTbl();
    }

    @Test
    public void testLimitOFFSET() {
        String t = "Select * from Animals LIMIT 100 OFFSET 50";
        byte[] bytes = t.getBytes();
        parser.parse(bytes, context);
        assertEquals(1, context.getTableCount());
        assertEquals(true, context.hasLimit());
        assertEquals(50, context.getLimitStart());
        assertEquals(100, context.getLimitCount());
    }

    @Test
    public void testSubqueryLimit() {
        String t = "DELETE \n" +
                "FROM posts \n" +
                "WHERE id not in (\n" +
                "      SELECT * FROM (\n" +
                "            SELECT id \n" +
                "            FROM posts \n" +
                "            ORDER BY timestamp desc limit 0, 15\n" +
                "      ) \n" +
                "      as t);";
        byte[] bytes = t.getBytes();
        parser.parse(bytes, context);
        assertEquals(2, context.getTableCount());
        assertEquals(true, context.hasLimit());
        assertEquals(0, context.getLimitStart());
        assertEquals(15, context.getLimitCount());
    }

    @Test
    public void testLimit5() {
        String t = "SELECT * FROM table LIMIT 5";
        byte[] bytes = t.getBytes();
        parser.parse(bytes, context);
        assertEquals(1, context.getTableCount());
        assertEquals(true, context.hasLimit());
        assertEquals(0, context.getLimitStart());
        assertEquals(5, context.getLimitCount());
    }

    @Test
    public void testLimitRange() {
        String t = "SELECT * FROM table LIMIT 95,-1";
        byte[] bytes = t.getBytes();
        parser.parse(bytes, context);
        assertEquals(1, context.getTableCount());
        assertEquals(true, context.hasLimit());
        assertEquals(95, context.getLimitStart());
        assertEquals(-1, context.getLimitCount());
    }

    @Test
    public void testNormalSelect() {
        String t = "SELECT * FROM a;# This comment continues to the end of line";
        parser.parse(t.getBytes(), context);
        assertEquals(1, context.getTableCount());
    }

    @Test
    public void testMultiTableSelect() {
        String t = "SELECT a.*, b.* FROM tbl_A a,tbl_B b , tbl_C c;#This comment continues to the end of line\n ";
        parser.parse(t.getBytes(), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(3, context.getTableCount());
    }

    @Test
    public void testJoinSelect() {
        String t = "SELECT a.*, b.* FROM tbl_A as a left join tbl_B b on b.id=a.id;-- This comment continues to the end of line";
        parser.parse(t.getBytes(), context);
        assertEquals(2, context.getTableCount());
    }

    @Test
    public void testNestSelect() {
        String sql = "SELECT a fROm ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));";
        parser.parse(sql.getBytes(), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(5, context.getTableCount());
        assertEquals("ab", context.getTableName(0));
        assertEquals("ff", context.getTableName(1));
        assertEquals("tbl_bb", context.getTableName(2));
        assertEquals("ccc", context.getTableName(3));
        assertEquals("dddd", context.getTableName(4));
        assertEquals("schema_bb", context.getSchemaName(2));
    }

    @Test
    public void testNormalUpdate() {
        String sql = "UPDATE tbl_A set name='kaiz' where name='nobody';";
        parser.parse(sql.getBytes(), context);
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testPriorityUpdate() {
        String sql = "UPDATE LOW_PRIORITY tbl_A set name='kaiz' where name='nobody';";
        parser.parse(sql.getBytes(), context);
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testIgnoreUpdate() {
        String sql = "UPDATE IGNORE tbl_A set name='kaiz' where name='nobody';";
        parser.parse(sql.getBytes(), context);
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalDelete() {
        String sql = "DELETE FROM tbl_A WHERE name='nobody';";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.DELETE_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalInsert() {
        String sql = "INSERT INTO tbl_A (`name`) VALUES ('kaiz');";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.INSERT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalInsert2() {
        String sql = "INSERT `schema`.`tbl_A` (`name`) VALUES ('kaiz');";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.INSERT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testIgnoreInsert() {
        String sql = "INSERT IGNORE tbl_A (`name`) VALUES ('kaiz');";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.INSERT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testDelayedInsert() {
        String sql = "INSERT DELAYED INTO tbl_A (`name`) VALUES ('kaiz');";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.INSERT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testPriorityInsert() {
        String sql =
                "INSERT LOW_PRIORITY INTO tbl_A (`name`) VALUES ('kaiz');\n"
                        + "Select * from abc;Update tbl_B set a = 'im' where a = 'no';";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.INSERT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(context.getRealSQL(0),
                "INSERT LOW_PRIORITY INTO tbl_A (`name`) VALUES ('kaiz');");
        assertEquals(context.getRealSQL(1), "Select * from abc;");
        assertEquals(context.getRealSQL(2), "Update tbl_B set a = 'im' where a = 'no';");

        String sql2 = "INSERT HIGH_PRIORITY INTO tbl_B (`name`) VALUES ('kaiz');";
        parser.parse(sql2.getBytes(), context);
        assertEquals(BufferSQLContext.INSERT_SQL, context.getSQLType());
        assertEquals("tbl_B", context.getTableName(0));
        assertEquals(context.getRealSQL(0), sql2);
    }

    @Test
    public void testNormalReplace() {
        String sql = "Replace into tbl_A (`name`) VALUES ('kaiz');";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.REPLACE_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalAlter() {
        String sql = "ALTER TABLE tbl_A ADD name VARCHAR(15) NULL;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.ALTER_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testDropAlter() {
        String sql = "ALTER TABLE tbl_A DROP name VARCHAR(15) NULL;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.ALTER_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalDrop() {
        String sql = "DROP TABLE IF EXISTS tbl_A;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.DROP_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalCreate() {
        String sql = "CREATE TABLE IF NOT EXISTS tbl_A ( Id INT NOT NULL UNIQUE PRIMARY KEY, name VARCHAR(20) NOT NULL;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.CREATE_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalTruncate() {
        String sql = "Truncate TABLE IF EXISTS tbl_A;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.TRUNCATE_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testCase01() {
        String sql = "select sum(convert(borrow_principal/100, decimal(18,2))) '借款本金'\n" +
                "    from s_user_borrow_record_status\n" +
                "    where 1=1\n" +
                "    and create_at >= '2017-01-04 00:00:00'\n" +
                "    and create_at <= '2017-01-04 23:59:59';";
        parser.parse(sql.getBytes(), context);
        assertEquals("s_user_borrow_record_status", context.getTableName(0));
    }

    @Test
    public void testCase02() {
        parser.parse(sql1.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(8, context.getTableCount());
    }

    @Test
    public void testCase03() {
        parser.parse(sql2.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(17, context.getTableCount());
    }

    @Test
    public void testCase04() {
        parser.parse(sql3.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(5, context.getTableCount());
    }

    @Test
    public void testCase05() {
        parser.parse(sql4.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(6, context.getTableCount());
    }

    @Test
    public void testCase06() {
        parser.parse(sql5.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(2, context.getTableCount());
    }

    @Test
    public void testCase07() {
        parser.parse(sql6.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(2, context.getTableCount());
    }

    @Test
    public void testCase08() {
        parser.parse(sql7.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(2, context.getTableCount());
    }

    @Test
    public void testCase09() {
        parser.parse(sql8.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(2, context.getTableCount());
    }

    @Test
    public void testCase10() {
        parser.parse(sql9.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(2, context.getTableCount());
    }

    @Test
    public void testCase11() {
        parser.parse(sql10.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(1, context.getTableCount());
    }

    @Test
    public void testCase12() {
        parser.parse(sql11.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(22, context.getTableCount());
    }

    @Test
    public void testNormalComment() {
        String sql = "select * from tbl_A, -- 单行注释\n" +
                "tbl_B b, #另一种单行注释\n" +
                "/*\n" +
                "tbl_C\n" +
                "*/ tbl_D d;";
        parser.parse(sql.getBytes(), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        assertEquals(3, context.getTableCount());
    }


    @Test
    public void testDoubleQuoteString() {
        String sql = "select id as \"select * from fake_tbl;\" from tbl_A;";
        parser.parse(sql.getBytes(), context);
        assertEquals(1, context.getTableCount());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testSingleQuoteString() {
        String sql = "select id as 'select * from fake_tbl;' from tbl_A;";
        parser.parse(sql.getBytes(), context);
        assertEquals(1, context.getTableCount());
        assertEquals("tbl_A", context.getTableName(0));
    }

    /**
     * @throws Exception
     */
    @Test
    public void testAnnotationBalance() {
        String sql = "/* mycat:balance type=master*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_BALANCE, context.getAnnotationType());
        assertEquals(TokenHash.MASTER, context.getAnnotationValue(BufferSQLContext.ANNOTATION_BALANCE));
    }

    /**
     * @throws Exception
     */
    @Test
    public void testReplica() {
        String sql = "/* mycat:replica name=dn1*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_REPLICA_NAME, context.getAnnotationType());
        assertEquals(MatchMethodGenerator.genHash("dn1".toCharArray()), context.getAnnotationValue(BufferSQLContext.ANNOTATION_REPLICA_NAME));
    }

    @Test
    public void testAnnotationDBType() {
        String sql = "/* MyCAT:DB_Type=Master*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_DB_TYPE, context.getAnnotationType());
        assertEquals(TokenHash.MASTER, context.getAnnotationValue(BufferSQLContext.ANNOTATION_DB_TYPE));
    }

    @Test
    public void testAnnotationSchema() {
        String sql = "/* MyCAT:schema=testDB*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_SCHEMA, context.getAnnotationType());
        assertEquals(MatchMethodGenerator.genHash("testDB".toCharArray()), context.getAnnotationValue(BufferSQLContext.ANNOTATION_SCHEMA));
    }

    @Test
    public void testAnnotationDataNode() {
        String sql = "/* MyCAT:DataNode=dn1*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_DATANODE, context.getAnnotationType());
        assertEquals(MatchMethodGenerator.genHash("dn1".toCharArray()), context.getAnnotationValue(BufferSQLContext.ANNOTATION_DATANODE));
    }

    @Test
    public void testMergeAnnotationDataNode() {
        String sql = "/* MyCAT:" +
                "merge(" +
                "dataNodes=dn1,dn2 " +
                "groupColumns=a,b,c \n" +
                "mergeColumns=a:sum,c:MAX\n" +
                "having=a gt c \n" +
                "order=a:desc,b:asc\n" +
                "limitStart=0 \n" +
                "limitSize=100)" +
                "*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_MERGE, context.getAnnotationType());
        MergeAnnotation mergeAnnotation = context.getMergeAnnotation();

        assertEquals.accept(new String[]{"dn1", "dn2"}, mergeAnnotation.getDataNodes());
        assertEquals.accept(new String[]{"a", "b", "c"}, mergeAnnotation.getGroupColumns());
        assertEquals.accept(new String[]{"a", "c"}, mergeAnnotation.getMergeColumns());
        assertEquals("sum", mergeAnnotation.getMergeType(0));
        assertEquals("MAX", mergeAnnotation.getMergeType(1));
        assertEquals("select * from tbl_A where id=1;", context.getRealSQL(0));


        assertEquals("a", mergeAnnotation.getLeft());
        assertEquals("gt", mergeAnnotation.getOp());
        assertEquals("c", mergeAnnotation.getRight());

        assertEquals("a", mergeAnnotation.getOrderColumn(0));
        assertEquals("b", mergeAnnotation.getOrderColumn(1));
        assertEquals(MergeAnnotation.OrderType.DESC, mergeAnnotation.getOrderType(0));
        assertEquals(MergeAnnotation.OrderType.ASC, mergeAnnotation.getOrderType(1));

        assertEquals(0, mergeAnnotation.getLimitStart());
        assertEquals(100, mergeAnnotation.getLimitSize());
    }

    @Test
    public void testMergeAnnotationDataNodeWithoutGroup() {
        String sql = "/* MyCAT:" +
                "merge(" +
                "dataNodes=dn1,dn2 " +
                "order=a:desc,b:asc " +
                "limitStart=0 \n" +
                "limitSize=100)" +
                "*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);

        MergeAnnotation mergeAnnotation = context.getMergeAnnotation();

        assertEquals.accept(new String[]{"dn1", "dn2"}, mergeAnnotation.getDataNodes());

        assertEquals("a", mergeAnnotation.getOrderColumn(0));
        assertEquals("b", mergeAnnotation.getOrderColumn(1));
        assertEquals(MergeAnnotation.OrderType.DESC, mergeAnnotation.getOrderType(0));
        assertEquals(MergeAnnotation.OrderType.ASC, mergeAnnotation.getOrderType(1));
        assertEquals(0, mergeAnnotation.getLimitStart());
        assertEquals(100, mergeAnnotation.getLimitSize());
    }

    @Test
    public void testMergeAnnotationDataNodeWithoutOrder() {
        String sql = "/* MyCAT:" +
                "merge(" +
                "dataNodes=dn1,dn2 " +
                "groupColumns=a,b,c \n" +
                "mergeColumns=a:min,c:max \n" +
                "having=a gt c \n" +
                ")" +
                "*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_MERGE, context.getAnnotationType());
        MergeAnnotation mergeAnnotation = context.getMergeAnnotation();
        assertEquals("min", mergeAnnotation.getMergeType(0));
        assertEquals("max", mergeAnnotation.getMergeType(1));

        assertEquals.accept(new String[]{"dn1", "dn2"}, mergeAnnotation.getDataNodes());
        assertEquals.accept(new String[]{"a", "b", "c"}, mergeAnnotation.getGroupColumns());
        assertEquals.accept(new String[]{"a", "c"}, mergeAnnotation.getMergeColumns());

        assertEquals("a", mergeAnnotation.getLeft());
        assertEquals("gt", mergeAnnotation.getOp());
        assertEquals("c", mergeAnnotation.getRight());
    }

    @Test
    public void testMergeAnnotationDataNodeWithoutHaving() {
        String sql = "/* MyCAT:" +
                "merge(" +
                "dataNodes=dn1,dn2 " +
                "groupColumns=a,b,c \n" +
                "mergeColumns=a:max,c:min \n" +
                "order=a:desc,b:asc\n" +
                "limitStart=0 \n" +
                "limitSize=100)" +
                "*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_MERGE, context.getAnnotationType());
        MergeAnnotation mergeAnnotation = context.getMergeAnnotation();
        assertEquals("max", mergeAnnotation.getMergeType(0));
        assertEquals("min", mergeAnnotation.getMergeType(1));

        assertEquals.accept(new String[]{"dn1", "dn2"}, mergeAnnotation.getDataNodes());
        assertEquals.accept(new String[]{"a", "b", "c"}, mergeAnnotation.getGroupColumns());
        assertEquals.accept(new String[]{"a", "c"}, mergeAnnotation.getMergeColumns());

        assertEquals("a", mergeAnnotation.getOrderColumn(0));
        assertEquals("b", mergeAnnotation.getOrderColumn(1));
        assertEquals(MergeAnnotation.OrderType.DESC, mergeAnnotation.getOrderType(0));
        assertEquals(MergeAnnotation.OrderType.ASC, mergeAnnotation.getOrderType(1));
        assertEquals(0, mergeAnnotation.getLimitStart());
        assertEquals(100, mergeAnnotation.getLimitSize());
    }

    @Test
    public void testAnnotationCatlet() {
        String sql = "/* MyCAT:catlet=demo.catlets.ShareJoin*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_CATLET, context.getAnnotationType());
        assertEquals("demo.catlets.ShareJoin", context.getCatletName());
    }

    @Test
    public void testAnnotationSQL() {
        String sql = "/* MyCAT:sql=select id from tbl_B where id = 101*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_SQL, context.getAnnotationType());
        //TODO 还需要完善提取SQL的内容和where条件
    }

    @Test
    public void testSimpleMultiSQL() {
        String sql = "insert tbl_A(id, val) values(1, 2);\n" +
                "insert tbl_B(id, val) values(2, 2);\n" +
                "insert tbl_C(id, val) values(3, 2);\n" +
                "insert tbl_D(id, val) values(4, 2);\n" +
                "insert tbl_E(id, val) values(5, 2);\n" +
                "insert tbl_F(id, val) values(6, 2);\n" +
                "insert tbl_G(id, val) values(7, 2);\n" +
                "insert tbl_H(id, val) values(8, 2);\n" +
                "insert tbl_I(id, val) values(9, 2);\n" +
                "insert tbl_J(id, val) values(10, 2);\n" +
                "insert tbl_K(id, val) values(11, 2);\n" +
                "insert tbl_L(id, val) values(12, 2);\n" +
                "insert tbl_M(id, val) values(13, 2);\n" +
                "insert tbl_N(id, val) values(14, 2);\n" +
                "insert tbl_O(id, val) values(15, 2);\n" +
                "insert tbl_P(id, val) values(16, 2);\n" +
                "insert tbl_Q(id, val) values(17, 2);\n" +
                "insert tbl_R(id, val) values(18, 2);\n" +
                "SELECT id, val FROM tbl_S where id=19;\n" +
                "insert tbl_T(id, val) values(20, 2)";
        parser.parse(sql.getBytes(), context);
        assertEquals(20, context.getSQLCount());
        // context.setSQLIdx(19);
        assertEquals("tbl_A", context.getSQLTableName(0, 0));
        assertEquals("tbl_S", context.getSQLTableName(18, 0));
        assertEquals("tbl_T", context.getSQLTableName(19, 0));
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType(18));
        assertEquals(BufferSQLContext.INSERT_SQL, context.getSQLType(19));
        assertEquals("insert tbl_A(id, val) values(1, 2);", context.getRealSQL(0));
        assertEquals("insert tbl_B(id, val) values(2, 2);", context.getRealSQL(1));
        assertEquals("insert tbl_R(id, val) values(18, 2);", context.getRealSQL(17));
        assertEquals("SELECT id, val FROM tbl_S where id=19;", context.getRealSQL(18));
    }

    @Test
    public void testUseSchemaSQL() {
        String sql = "USE `mycat`;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.USE_SQL, context.getSQLType());
        assertEquals("mycat", context.getSchemaName(0));
    }

    @Test
    public void testGetRealSQL() {
        String sql =
                "/* MyCAT:sql=select id from tbl_B where id = 101*/select * from tbl_A where id=1;update tbl_A set name = 'a' wnere id =1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_SQL, context.getAnnotationType());
        assertEquals(50, context.getRealSQLOffset(0));
        assertEquals("select * from tbl_A where id=1;", context.getRealSQL(0));
        assertEquals("update tbl_A set name = 'a' wnere id =1;", context.getRealSQL(1));
    }

    @Test
    public void testAnnotationCacheSQL() {
        String sql = "/* MyCAT:cacheresult  cache_time=1000 auto_refresh=true access_count=100*/select * from tbl_A where id=1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
        assertEquals(BufferSQLContext.ANNOTATION_SQL_CACHE, context.getAnnotationType());
        assertEquals(1000, context.getAnnotationValue(BufferSQLContext.ANNOTATION_CACHE_TIME));
        assertEquals(100, context.getAnnotationValue(BufferSQLContext.ANNOTATION_ACCESS_COUNT));
        assertEquals(TokenHash.TRUE, context.getAnnotationValue(BufferSQLContext.ANNOTATION_AUTO_REFRESH));
    }

    @Test
    public void testAnnotationCacheLimitSQL() {
        for (int i = 0; i < 1000; i++) {
//            System.out.println("testAnnotationCacheLimitSQL "+i);
            if (i % 2 == 1) {
                String sql = "/* MyCAT:cacheresult  cache_time=1000 auto_refresh=true access_count=100*/select a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z from tbl_A where id=1 limit 100;";
                parser.parse(sql.getBytes(), context);
                assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
                assertEquals("tbl_A", context.getTableName(0));
                assertEquals(BufferSQLContext.ANNOTATION_SQL_CACHE, context.getAnnotationType());
                assertEquals(1000, context.getAnnotationValue(BufferSQLContext.ANNOTATION_CACHE_TIME));
                assertEquals(100, context.getAnnotationValue(BufferSQLContext.ANNOTATION_ACCESS_COUNT));
                assertEquals(TokenHash.TRUE, context.getAnnotationValue(BufferSQLContext.ANNOTATION_AUTO_REFRESH));
                assertEquals(100, context.getLimitCount());
                assertEquals("select a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z from tbl_A where id=1 limit 100;", context.getRealSQL(0));
            } else {
                String sql = "/* MyCAT:cacheresult  cache_time=100 auto_refresh=true access_count=100*/select a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z from tbl_A where id=1 limit 10;";
                parser.parse(sql.getBytes(), context);
                assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
                assertEquals("tbl_A", context.getTableName(0));
                assertEquals(BufferSQLContext.ANNOTATION_SQL_CACHE, context.getAnnotationType());
                assertEquals(100, context.getAnnotationValue(BufferSQLContext.ANNOTATION_CACHE_TIME));
                assertEquals(100, context.getAnnotationValue(BufferSQLContext.ANNOTATION_ACCESS_COUNT));
                assertEquals(TokenHash.TRUE, context.getAnnotationValue(BufferSQLContext.ANNOTATION_AUTO_REFRESH));
                assertEquals(10, context.getLimitCount());
                assertEquals("select a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z from tbl_A where id=1 limit 10;", context.getRealSQL(0));
            }
        }

    }

    @Test
    public void testLoadDataSQL() {
        String sql = "load data  low_priority infile \"/home/mark/data.sql\" replace into table tbl_A;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.LOAD_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testLoadXMLSQL() {
        String sql =
                "load xml concurrent local infile \"/home/mark/data.xml\" ignore into table tbl_A;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.LOAD_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testSelectGlobalVarSQL() {
        String sql = "select @@version limit 1;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        assertEquals(sql, context.getRealSQL(0));
    }

    @Test
    public void testShowDatabasesSQL() {
        String sql = "show databases";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SHOW_DB_SQL, context.getSQLType());
        assertEquals(sql, context.getRealSQL(0));
    }

    @Test
    public void testShowVariables() {
        String sql = "show variables like 'profiling'";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SHOW_SQL, context.getSQLType());
        assertEquals(sql, context.getRealSQL(0));
    }

    @Test
    public void testSelectIntoSQL() {
        String sql = "select * into tbl_B from tbl_A;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_INTO_SQL, context.getSQLType());
        assertEquals("tbl_B", context.getTableName(0));
        assertEquals("tbl_A", context.getTableName(1));
    }

    @Test
    public void testSelectForUpdateSQL() {
        String sql = "select * from tbl_A for update;";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_FOR_UPDATE_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testCustomSQL() {
        String sql = "SELECT DATABASE()";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
        // assertEquals("sbtest1", context.getTableName(0));
    }

    @Test
    public void testTokenHash1() {
        String sql = "select * from tbl_A;";
        parser.parse(sql.getBytes(), context);
        assertEquals(TokenHash.FROM, context.getTokenHash(0, 2));
    }

    @Test
    public void testTokenHash2() {
        String sql = "select * from tbl_A; select * from tbl_B where id > 100;";
        parser.parse(sql.getBytes(), context);
        assertEquals(TokenHash.WHERE, context.getTokenHash(1, 4));
    }

    @Test
    public void testTokenType() {
        String sql = "SELECT * FROM tbl_A where id = 1234;";
        parser.parse(sql.getBytes(), context);
        assertEquals(Tokenizer.DIGITS, context.getTokenType(0, 7));
    }

    @Test
    public void testSelectItemList1() {
        String sql = "SELECT VERSION(), USER(), DATABASE()";
        parser.parse(sql.getBytes(), context);
        assertEquals(FunctionHash.VERSION, context.getSelectItem(0));
        assertEquals(FunctionHash.USER, context.getSelectItem(1));
        assertEquals(FunctionHash.DATABASE, context.getSelectItem(2));
    }

    @Test
    public void testKill() {
        String sql = "kill 19201";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.KILL_SQL, context.getSQLType());

        sql = "kill query 19201";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.KILL_QUERY_SQL, context.getSQLType());

        sql = "kill connection 19201";
        parser.parse(sql.getBytes(), context);
        assertEquals(BufferSQLContext.KILL_SQL, context.getSQLType());
    }

    @Test
    public void testHelp() {
        String sql = "help 'load data'";
        parser.parse(ByteBuffer.wrap(sql.getBytes()), 0, sql.length(), context);
        assertEquals(BufferSQLContext.HELP_SQL, context.getSQLType());
        assertEquals(TokenHash.HELP, context.getTokenHash(0, 0));
        assertEquals(Tokenizer2.STRINGS, context.getTokenType(0, 1));
    }

    @Test
    public void testHelp2() {
        String sql = "help load data";
        ByteArrayInterface src = new ByteBufferArray(sql.getBytes());
        parser.parse(src, context);
        assertEquals(BufferSQLContext.HELP_SQL, context.getSQLType());
        assertEquals(TokenHash.HELP, context.getTokenHash(0, 0));
        assertEquals(TokenHash.LOAD, context.getTokenHash(0, 1));
    }

    @Test
    public void testSimpleOrderby() {
//        String sql = "select id1, id2, id3 from tbl_A order by 1;";
        String sql = "select * from message order by id desc";
        ByteArrayInterface src = new ByteBufferArray(sql.getBytes());
        parser.parse(src, context);
        assertEquals(BufferSQLContext.SELECT_SQL, context.getSQLType());
    }



    private static final String sql1 = "select t3.*,ztd3.TypeDetailName as UseStateName\n" +
            "from\n" +
            "( \n" +
            " select t4.*,ztd4.TypeDetailName as AssistantUnitName\n" +
            " from\n" +
            " (\n" +
            "  select t2.*,ztd2.TypeDetailName as UnitName \n" +
            "  from\n" +
            "  (\n" +
            "   select t1.*,ztd1.TypeDetailName as MaterielAttributeName \n" +
            "   from \n" +
            "   (\n" +
            "    select m.*,r.RoutingName,u.username,mc.MoldClassName\n" +
            "    from dbo.D_Materiel as m\n" +
            "    left join dbo.D_Routing as r\n" +
            "    on m.RoutingID=r.RoutingID\n" +
            "    left join dbo.D_MoldClass as mc\n" +
            "    on m.MoldClassID=mc.MoldClassID\n" +
            "    left join dbo.D_User as u\n" +
            "    on u.UserId=m.AddUserID\n" +
            "   )as t1\n" +
            "   left join dbo.D_Type_Detail as ztd1 \n" +
            "   on t1.MaterielAttributeID=ztd1.TypeDetailID\n" +
            "  )as t2\n" +
            "  left join dbo.D_Type_Detail as ztd2 \n" +
            "  on t2.UnitID=ztd2.TypeDetailID\n" +
            " ) as t4\n" +
            " left join dbo.D_Type_Detail as ztd4 \n" +
            " on t4.AssistantUnitID=ztd4.TypeDetailID\n" +
            ")as t3\n" +
            "left join dbo.D_Type_Detail as ztd3 \n" +
            "on t3.UseState=ztd3.TypeDetailID";


    private static final String sql2 = "Select d.Fabric_No,\n" +
            "       f.MachineName,\n" +
            "       f.RawNo,\n" +
            "       f.OldRawNo,\n" +
            "       f.RawName,\n" +
            "       f.StructCode,\n" +
            "       p.WorkClass,\n" +
            "       d.DefectType,\n" +
            "       d.DefectName,\n" +
            "       f.InspectResult,\n" +
            "       Convert(Char(10), InspectDate, 20) As InspectDate,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          Convert(Decimal(28, 2),\n" +
            "                  (d.DefectEnd - d.DefectStart + 1) /\n" +
            "                  dbo.f_JT_CalcMinValue(LPair, RPair) * Allow_Qty)\n" +
            "         Else\n" +
            "          (d.DefectEnd - d.DefectStart + 1)\n" +
            "       End) As MLength,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          ISNULL((Select SUM(DefectEnd)\n" +
            "                   From FIInspectFabricDefects s\n" +
            "                  Where DefectStart >= d.DefectStart\n" +
            "                    And DefectStart <= d.DefectEnd\n" +
            "                    And Fabric_No = d.Fabric_No\n" +
            "                    And RecType = '疵点'),\n" +
            "                 0.00)\n" +
            "         Else\n" +
            "          ISNULL((Select SUM(DefectEnd - DefectStart + 1)\n" +
            "                   From FIInspectFabricDefects s\n" +
            "                  Where DefectStart >= d.DefectStart\n" +
            "                    And DefectStart <= d.DefectEnd\n" +
            "                    And DefectEnd >= d.DefectStart\n" +
            "                    And DefectEnd <= d.DefectEnd\n" +
            "                    And Fabric_No = d.Fabric_No\n" +
            "                    And RecType = '疵点'),\n" +
            "                 0.00)\n" +
            "       End) As DefectNum,\n" +
            "       Convert(Decimal(28, 2),\n" +
            "               (d.DefectEnd - d.DefectStart + 1.0) / (Case\n" +
            "                 When f.StructCode = 'JT' Then\n" +
            "                  dbo.f_JT_CalcMinValue(LPair, RPair)\n" +
            "                 Else\n" +
            "                  f.Allow_Qty\n" +
            "               End) * f.Allow_Wt) As MWeight,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 1)\n" +
            "         Else\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 1)\n" +
            "       End) As OneDefectName,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 1)\n" +
            "         Else\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 1)\n" +
            "       End) As OneDefect,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 2)\n" +
            "         Else\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 2)\n" +
            "       End) As TwoDefectName,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 2)\n" +
            "         Else\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 2)\n" +
            "       End) As TwoDefect,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 3)\n" +
            "         Else\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 3)\n" +
            "       End) As ThreeDefectName,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 3)\n" +
            "         Else\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 3)\n" +
            "       End) As ThreeDefect\n" +
            "  From FIInspectFabric f, FIInspectFabricDefects d, t_ORC_StanPersonSet P\n" +
            " Where d.RecType = '产量'\n" +
            "   And d.DefectType = p.PNo\n" +
            "   And f.Fabric_NO = d.Fabric_No\n" +
            " Order By d.Fabric_No, p.WorkClas\n";

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

    static String sql4 = "select \n" +
            "a.cust_id  cust_id ,\n" +
            "ifnull(f.cust_flag,'') cust_flag,\n" +
            "a.create_date  reg_time,\n" +
            "b.cust_reg_channel     reg_ditch_code,\n" +
            "ifnull(b.cust_reg_inviter,'')  cust_reg_inviter,\n" +
            "ifnull(e.cust_nname,'')  cust_inviter_nname,\n" +
            "a.cust_nname  cust_nname,\n" +
            "case   when  c.type=1  or   c.type=3 then 1 else 0 end is_weixin,\n" +
            "case   when  c.type=2  or   c.type=3 then 1 else 0 end is_weibo,\n" +
            "a.cust_phone cust_phone,\n" +
            "a.cust_status cust_status,\n" +
            "a.cust_location cust_location,\n" +
            "a.cust_sex cust_sex,\n" +
            "d.generalize_scene     generalize_scene,\n" +
            "d.useage_person useage_person,\n" +
            "ifnull(b.cust_reg_inviter_num,0)   generalize_num\n" +
            "from tbl_cust_info a inner join tbl_cust b on a.cust_id=b.cust_id\n" +
            "                         left join (select  cust_id, sum(type) type \n" +
            "\t\t\t\t\t\t\t\t\t  from  tbl_third_login  where   `type` in(1,2)  \n" +
            "                                      group by cust_id) c on c.cust_id=b.cust_id\n" +
            "                         left join  tbl_cust_info e  on b.cust_reg_inviter=e.cust_id\n" +
            "                         left  join tbl_check d on b.cust_reg_channel=d.ditch_code\n" +
            "                         left join tbl_cust_flag f on f.cust_id=a.cust_id\n" +
            "where a.update_timestamp>DATE_SUB(now(),  INTERVAL  ${N}    MINUTE) or  b.update_timestamp>DATE_SUB(now(),  INTERVAL  30  MINUTE) \n";

    static String sql5 = "select  \n" +
            "a_cust_id cust_id,\n" +
            "count(a_cust_id) friend_num \n" +
            "from tbl_friends\n" +
            "where    a_cust_id  in (select a_cust_id  from  tbl_friends\n" +
            "                        where update_timestamp>DATE_SUB(now(),  INTERVAL  30  MINUTE))\n" +
            "group by a_cust_id";

    static String sql6 = "select  \n" +
            "cust_id,\n" +
            "sum(cost) integral_total \n" +
            "from tbl_IHB \n" +
            "where    order_type=1 and  cust_id\n" +
            "in (select cust_id from  tbl_IHB  \n" +
            "    where update_timestamp>DATE_SUB(now(),  INTERVAL  30  MINUTE))\n" +
            "group by  cust_id ";

    static String sql7 = "select  \n" +
            "CUST_ID,\n" +
            "sum(PRICE)  amount,\n" +
            "count(1)  cnt\n" +
            "from  tbl_r\n" +
            "where cust_id in(select cust_id from   tbl_r where update_timestamp>DATE_SUB(now(),  INTERVAL  30  MINUTE))\n" +
            "group by CUST_ID";

    static String sql8 = "select  \n" +
            " tc_id CUST_ID ,\n" +
            "sum(rprice)  get_rb,\n" +
            "count(1)   get_rc ,\n" +
            "sum(p)  get_ra\n" +
            "from  tbl_r\n" +
            "where tc_id in(select tc_id from   tbl_r where update_timestamp>DATE_SUB(now(),  INTERVAL  30  MINUTE))\n" +
            "group by tc_id";

    static String sql9 = "select  CUST_ID,\n" +
            "sum(case packet_s when 0 then 1 else 0 end )  rc_cnt,\n" +
            "sum(case packet_s when 0 then packet_a else 0 end )  rc_amount,\n" +
            "sum(case packet_s when 1 then 1 else 0 end )  rc_cnt,\n" +
            "sum(case packet_s when 1 then packet_a else 0 end )  ra_amount \n" +
            "from tbl_r\n" +
            "where  packet_s  in (0,1)\n" +
            "and   cust_id in(select cust_id from  tbl_r where update_timestamp>DATE_SUB(now(),  INTERVAL  30  MINUTE))\n" +
            "group by CUST_ID";

    static String sql10 = "select \n" +
            "ifnull(max(CONVERT(forward_id,SIGNED)),0) max_forward_id1,\n" +
            "ifnull(max(CONVERT(forward_id,SIGNED)),0) max_forward_id2,\n" +
            "ifnull(max(CONVERT(forward_id,SIGNED)),0) max_forward_id3,\n" +
            "ifnull(max(CONVERT(forward_id,SIGNED)),0) max_forward_id0\n" +
            "from tbl_forward;";

    static String sql11 = "select  \n" +
            "a.club_up_1 forward_id,\n" +
            "d.cust_id cust_id,\n" +
            "d.cust_nname cust_nname,\n" +
            "e.id forward_info_id,\n" +
            "e.info_type forward_info_type,\n" +
            "e.info_context forward_info_context,\n" +
            "b.money forward_income,\n" +
            "c.transfer_price forward_price,\n" +
            "f.transfer_price beforward_price,\n" +
            "b.create_time    forward_date,\n" +
            "a.club_id beforward_id,\n" +
            "f.create_date   beforward_date,\n" +
            "1 beforward_level,\n" +
            "date(b.create_time) date ,\n" +
            "now() create_date \n" +
            "from  rrz_user_forward a\n" +
            "inner join  tbl_forward_s b  on a.club_id=b.club_id \n" +
            "inner join  tbl_cust_2_info c  on  a.club_up_1=c.id\n" +
            "inner join tbl_cust_info d on  c.cust_id=d.cust_id\n" +
            "inner join tbl_info  e on a.info_id=e.id\n" +
            "inner join tbl_cust_2_info f on  a.club_id=f.id                     \n" +
            "where  a.club_up_1 is not  null   and  b.type=1   and  a.club_id>100\n" +
            "union all  \n" +
            "select  \n" +
            "a.club_up_2 forwarded_id,\n" +
            "d.cust_id cust_id,\n" +
            "d.cust_nname cust_nname,\n" +
            "e.id forward_info_id,\n" +
            "e.info_type forward_info_type,\n" +
            "e.info_context forward_info_context,\n" +
            "b.money forward_income,\n" +
            "c.transfer_price forward_price,\n" +
            "f.transfer_price beforward_price,\n" +
            "b.create_time    forward_date,\n" +
            "a.club_id beforward_id,\n" +
            "f.create_date   beforward_date,\n" +
            "2 beforward_level,\n" +
            "date(b.create_time) date ,\n" +
            "now() create_date \n" +
            "from  rrz_user_forward a\n" +
            "inner join  tbl_forward_s b  on a.club_id=b.club_id \n" +
            "inner join  tbl_cust_2_info c  on  a.club_up_2=c.id\n" +
            "inner join tbl_cust_info d on  c.cust_id=d.cust_id\n" +
            "inner join tbl_info  e on a.info_id=e.id\n" +
            "inner join tbl_cust_2_info f on  a.club_id=f.id                     \n" +
            "where  a.club_up_2 is not  null   and  b.type=2    and  a.club_id>100\n" +
            "union all\n" +
            "select  \n" +
            "a.club_up_3 forwarded_id,\n" +
            "d.cust_id cust_id,\n" +
            "d.cust_nname cust_nname,\n" +
            "e.id forward_info_id,\n" +
            "e.info_type forward_info_type,\n" +
            "e.info_context forward_info_context,\n" +
            "b.money forward_income,\n" +
            "c.transfer_price forward_price,\n" +
            "f.transfer_price beforward_price,\n" +
            "b.create_time    forward_date,\n" +
            "a.club_id beforward_id,\n" +
            "f.create_date   beforward_date,\n" +
            "3 beforward_level,\n" +
            "date(b.create_time) date ,\n" +
            "now() create_date \n" +
            "from  rrz_user_forward a\n" +
            "inner join  tbl_forward_s b  on a.club_id=b.club_id \n" +
            "inner join  tbl_cust_2_info c  on  a.club_up_3=c.id\n" +
            "inner join tbl_cust_info d on  c.cust_id=d.cust_id\n" +
            "inner join tbl_info  e on a.info_id=e.id\n" +
            "inner join tbl_cust_2_info f on  a.club_id=f.id                     \n" +
            "where  a.club_up_3 is not  null   and  b.type=3     and  a.club_id>100\n" +
            "union all\n" +
            "select  \n" +
            "a.club_id forwarded_id,\n" +
            "d.cust_id cust_id,\n" +
            "d.cust_nname cust_nname,\n" +
            "e.id forward_info_id,\n" +
            "e.info_type forward_info_type,\n" +
            "e.info_context forward_info_context,\n" +
            "0 forward_income,\n" +
            "c.transfer_price forward_price,\n" +
            "0 beforward_price,\n" +
            "c.create_date    forward_date,\n" +
            "0 beforward_id,\n" +
            "c.create_date   beforward_date,\n" +
            "0 beforward_level,\n" +
            "date(c.create_date) date ,\n" +
            "now() create_date \n" +
            "from  rrz_user_forward a\n" +
            "inner join  tbl_cust_2_info c  on  a.club_id=c.id\n" +
            "inner join tbl_cust_info d on  c.cust_id=d.cust_id\n" +
            "inner join tbl_info  e on a.info_id=e.id    \n" +
            "where     a.club_id>100";
}

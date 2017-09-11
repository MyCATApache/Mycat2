package io.mycat.mycat2.sqlparser;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jamie on 2017/8/31.
 */
public class DCLSQLParseTest extends TestCase {
    /**
     GRANT
     priv_type [(column_list)]
     [, priv_type [(column_list)]] ...
     ON [object_type] priv_level
     TO user [auth_option] [, user [auth_option]] ...
     [REQUIRE {NONE | tls_option [[AND] tls_option] ...}]
     [WITH {GRANT OPTION | resource_option} ...]

     GRANT PROXY ON user
     TO user [, user] ...
     [WITH GRANT OPTION]

     object_type: {
     TABLE
     | FUNCTION
     | PROCEDURE
     }

     priv_level: {
     *
     | *.*
     | db_name.*
     | db_name.tbl_name
     | tbl_name
     | db_name.routine_name
     }

     user:
     (see Section 6.2.3, “Specifying Account Names”)

     auth_option: {
     IDENTIFIED BY 'auth_string'
     | IDENTIFIED WITH auth_plugin
     | IDENTIFIED WITH auth_plugin BY 'auth_string'
     | IDENTIFIED WITH auth_plugin AS 'hash_string'
     | IDENTIFIED BY PASSWORD 'hash_string'
     }

     tls_option: {
     SSL
     | X509
     | CIPHER 'cipher'
     | ISSUER 'issuer'
     | SUBJECT 'subject'
     }

     resource_option: {
     | MAX_QUERIES_PER_HOUR count
     | MAX_UPDATES_PER_HOUR count
     | MAX_CONNECTIONS_PER_HOUR count
     | MAX_USER_CONNECTIONS count
     }
     *
     */
    @Test
    public void testGrantPrivType() throws Exception {
        String t = " GRANT\n" +
                "     ALL " +
                "     \n" +
                "     ON FUNCTION db_name.tbl_name\n" +
                "     TO user IDENTIFIED BY 'auth_string' , user IDENTIFIED BY PASSWORD 'hash_string'\n" +
                "     REQUIRE SSL AND SUBJECT 'subject'\n" +
                "     WITH MAX_QUERIES_PER_HOUR 1;";
        test(t);
    }
    @Test
    public void testGrantPrivType2() throws Exception {
        String t = " GRANT\n" +
                "     CREATE TEMPORARY TABLES (a,b,e)\n" +
                "     , DROP (c,d)\n" +
                "     ON TABLE *\n" +
                "     TO user IDENTIFIED WITH auth_plugin AS 'hash_string'" +
                "     REQUIRE NONE\n" +
                "     WITH GRANT OPTION ;";
        test(t);
    }

    /**
     *   GRANT
     priv_type [(column_list)]
     [, priv_type [(column_list)]] ...
     ON [object_type] priv_level
     TO user [auth_option] [, user [auth_option]] ...
     [REQUIRE {NONE | tls_option [[AND] tls_option] ...}]
     [WITH {GRANT OPTION | resource_option} ...]
     */
    @Test
    public void testGrantPrivType3() throws Exception {
        String t = " GRANT\n" +
                "     CREATE TEMPORARY TABLES " +
                "     " +
                "     ON  *\n" +
                "     TO user" +
                "     REQUIRE NONE\n" +
                "     ;";
        test(t);
    }

    /**
     GRANT PROXY ON user
     TO user [, user] ...
     [WITH GRANT OPTION]
     */
    @Test
    public void testGrantProxy() throws Exception {
        String t = "GRANT PROXY ON user\n" +
                "     TO user2 , user3,,user4\n" +
                "     WITH GRANT OPTION;";
        test(t);
    }
    @Test
    public void testGrantProxy2() throws Exception {
        String t = "GRANT PROXY ON user\n" +
                "     TO user2 , user3,,user4\n" +
                "     ;";
        test(t);
    }
    /**
     * REVOKE
     * priv_type [(column_list)]
     * [, priv_type [(column_list)]] ...
     * ON [object_type] priv_level
     * FROM user [, user] ...
     ***/
    public  void testRevokePrivType()throws Exception{
        String t = "REVOKE\n" +
                " REPLICATION SLAVE (a)\n" +
                " , SHOW DATABASES (b,c,f,g)\n" +
                " ON PROCEDURE *.*\n" +
                "FROM user ,user2, user3;";
        test(t);
    }
    public  void testRevokePrivType2()throws Exception{
        String t = "REVOKE\n" +
                " REPLICATION SLAVE " +
                " ON  *.*\n" +
                "FROM user;";
        test(t);
    }

    /**
     * REVOKE PROXY ON user
     * FROM user [, user] ...
     */
    public  void testRevokeProxy()throws Exception{
        String t = "REVOKE PROXY ON user\n" +
                " FROM user1 , user2,user3;";
        test(t);
    }
    /***
     REVOKE ALL [PRIVILEGES], GRANT OPTION
     FROM user [, user] ...
     **/
    public  void testRevokeAll()throws Exception{
        String t = "REVOKE ALL PRIVILEGES, GRANT OPTION\n" +
                "     FROM user1 , user2;";
        test(t);
    }
    public  void testRevokeAll2()throws Exception{
        String t = "REVOKE ALL , GRANT OPTION\n" +
                "     FROM user1;";
        test(t);
    }



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

}

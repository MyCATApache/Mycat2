package io.mycat.mycat2.e2e.textProtocol;

import io.mycat.mycat2.e2e.BaseSQLExeTest;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

/**
 * cjw
 */
public class TextProtocolTest extends BaseSQLExeTest {

    @Test
    public void test_COM_SLEEP() {
        using(c -> {
                    Exception err = null;
                    try {
                        c.createStatement().execute("SLEEP;");
                    } catch (Exception e) {
                        err = e;
                    }
                    Assert.assertNotNull(err);
                }
        );
    }

    @Test
    public void test_COM_QUIT() {
        using(c -> c.close());
    }

    /**
     * @todo jdbc改变schema
     */
    @Test
    public void test_INIT_DB() {
        using(c -> {
                    c.createStatement().executeUpdate("use db1;");
                }
        );
    }

    static final String SECURE_FILE_PRIV = "D:/mysql-8.0.12-winx64/";

    @Test
    public void test_LOCAL_INFILE() {
        String path = SECURE_FILE_PRIV + ThreadLocalRandom.current().nextInt(0, 1024);
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("SELECT * FROM `db1`.`travelrecord` INTO OUTFILE '" + path + "'");
                    c.createStatement().executeUpdate("truncate table `db1`.`travelrecord`");
                    String sql = "load data infile '" +
                            path +
                            "' into table `db1`.`travelrecord` fields terminated by '\t'";
                    System.out.println(sql);
                    statement.execute(sql);
                }
        );
    }

    @Test
    public void test_LOAD_DATA_LOCAL_INFILE() {
        String path = SECURE_FILE_PRIV + ThreadLocalRandom.current().nextInt(0, 1024);
        using(c -> {
                    Statement statement = c.createStatement();
                    statement.execute("SELECT * FROM `db1`.`travelrecord` INTO OUTFILE '" + path + "'");
                    c.createStatement().executeUpdate("truncate table `db1`.`travelrecord`");
                    String sql = "LOAD DATA LOCAL INFILE '" +
                            path +
                            "' INTO TABLE `db1`.`travelrecord`;";
                    System.out.println(sql);
                    statement.execute(sql);
                }
        );
    }

    @Test
    public void test_COM_FLEID_LIST() {
        testFieldList();
    }

    @Test
    public void test_COM_CREATE_OR_DB() {
        using(c -> {
                    Statement statement = c.createStatement();
                    int i = ThreadLocalRandom.current().nextInt(0, 1024);
                    String db = "my_db" + i;
                    statement.execute("CREATE DATABASE " + db);
                    statement.execute("DROP DATABASE " + db);
                }
        );
    }
    //p334


}

package io.mycat.mycat2.e2e;

import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;

/**
 * @author : zhuqiang
 * @version : V1.0
 * @date : 2018/11/5 22:54
 */
public class ShowSQLTest extends BaseSQLTest {
    /** SHOW PROCEDURE STATUS */
    @Test
    public void showProcedureStatus() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW PROCEDURE STATUS");
            Assert.assertTrue(resultSet.next());
        });
    }

    @Test
    public void showProcedureStatus2() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW PROCEDURE STATUS LIKE 'multi'");
            Assert.assertTrue(resultSet.next());
        });
    }

    /** SHOW FUNCTION STATUS */
    @Test
    public void showFunctionStatus() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW FUNCTION STATUS");
            Assert.assertTrue(resultSet.next());
        });
    }

    @Test
    public void showFunctionStatus2() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW FUNCTION STATUS like 'format_bytes'");
            Assert.assertTrue(resultSet.next());
        });
    }
}

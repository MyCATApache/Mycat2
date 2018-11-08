package io.mycat.mycat2.e2e;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author : zhuqiang
 * @version : V1.0
 * @date : 2018/11/7 21:38
 */
public class DataDefinitionSQLTest extends BaseSQLTest {
    /** SQLCOM_CREATE_VIEW */
    @Test
    public void sqlcomCreateView() {
        using(c -> {
            boolean flag = c.createStatement().execute("CREATE VIEW db1.v AS SELECT * FROM travelrecord;");
            // 第一个是 resultSet 对象才会是 true
            Assert.assertFalse(flag);
        });
    }

    // 上面测试失败，有可能已经存在了，使用下面的测试可以覆盖
    @Test
    public void sqlcomCreateView2() {
        using(c -> {
            boolean flag = c.createStatement().execute("CREATE OR REPLACE VIEW db1.v AS SELECT * FROM travelrecord where id = 1;");
            Assert.assertFalse(flag);
        });
    }

    /** SQLCOM_DROP_VIEW */
    @Test
    public void sqlcomDropView() {
        using(c -> {
            boolean flag = c.createStatement().execute("DROP VIEW IF EXISTS db1.v");
            Assert.assertFalse(flag);
        });
    }

    /** SQLCOM_CREATE_TRIGGER */
    @Test
    public void sqlcomCreateTrigger() {
        using(c -> {
            // 在插入之前对 fee 字段增加1
            boolean flag = c.createStatement().execute("CREATE TRIGGER fee_update_to_days BEFORE INSERT ON travelrecord FOR EACH ROW\n" +
                    "SET new.fee = new.fee+1");
            Assert.assertFalse(flag);
        });
    }
    /** SQLCOM_DROP_TRIGGER */
    @Test
    public void sqlcomDropTrigger() {
        using(c -> {
            boolean flag = c.createStatement().execute("DROP TRIGGER  IF EXISTS db1.fee_update_to_days");
            Assert.assertFalse(flag);
        });
    }
}

package io.mycat.mycat2.e2e;

import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author : zhuqiang
 * @version : V1.0
 * @date : 2018/11/7 21:38
 */
public class DataDefinitionSQLTest extends BaseSQLTest {
    /** SQLCOM_CREATE_VIEW */
    public void sqlcomCreateView() {
        using(c -> {
            boolean flag = c.createStatement().execute("CREATE VIEW db1.v AS SELECT * FROM travelrecord;");
            // 第一个是 resultSet 对象才会是 true
            Assert.assertFalse(flag);
        });
    }

    // 上面测试失败，有可能已经存在了，使用下面的测试可以覆盖
    public void sqlcomCreateView2() {
        using(c -> {
            boolean flag = c.createStatement().execute("CREATE OR REPLACE VIEW db1.v AS SELECT * FROM travelrecord where id = 1;");
            Assert.assertFalse(flag);
        });
    }

    /** SQLCOM_DROP_VIEW */
    public void sqlcomDropView() {
        using(c -> {
            boolean flag = c.createStatement().execute("DROP VIEW IF EXISTS db1.v");
            Assert.assertFalse(flag);
        });
    }

    /** SQLCOM_CREATE_TRIGGER */
    public void sqlcomCreateTrigger() {
        using(c -> {
            // 在插入之前对 fee 字段增加1
            boolean flag = c.createStatement().execute("CREATE TRIGGER fee_update_to_days BEFORE INSERT ON travelrecord FOR EACH ROW\n" +
                    "SET new.fee = new.fee+1");
            Assert.assertFalse(flag);
        });
    }
    /** SQLCOM_DROP_TRIGGER */
    public void sqlcomDropTrigger() {
        using(c -> {
            boolean flag = c.createStatement().execute("DROP TRIGGER  IF EXISTS db1.fee_update_to_days");
            Assert.assertFalse(flag);
        });
    }
    
    
    
    /**
	 * UNINSTALL PLUGIN
	 */
    public void uninstallPlugin() {
	    using(c -> {
	        int resultSet = c.createStatement().executeUpdate("UNINSTALL PLUGIN validate_password");
	        Assert.assertEquals(0, resultSet);
	    });
    }
    
    /**
	 * INSTALL PLUGIN
	 */
    public void installPlugin() {
	    using(c -> {
	        int resultSet = c.createStatement().executeUpdate("INSTALL PLUGIN validate_password SONAME 'validate_password.dll'");
	        Assert.assertEquals(0, resultSet);
	    });
    }
    
    
    /**
     * SHOW PLUGINS
     */
    public void showPlugins() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW PLUGINS");
            Assert.assertTrue(resultSet.next());
        });
    }
    
    
    
    /**
	 * CREATE EVENT
	 */
    public void createEvent() {
	    using(c -> {
	        int resultSet = c.createStatement().executeUpdate("CREATE EVENT travelrecord_event ON SCHEDULE EVERY 10 SECOND DO INSERT INTO travelrecord(user_id,traveldate,fee,days) VALUES(5,NOW(),6,7)");
	        Assert.assertEquals(0, resultSet);
	    });
    }
    
    
    /**
     * SHOW CREATE EVENT
     */
    public void showCreateEvent() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW CREATE EVENT travelrecord_event");
            Assert.assertTrue(resultSet.next());
        });
    }
    
    
    /**
   	 * ALTER EVENT
   	 */
   public void alterEvent() {
    using(c -> {
        int resultSet = c.createStatement().executeUpdate("ALTER EVENT travelrecord_event ON SCHEDULE EVERY 10 SECOND DO INSERT INTO travelrecord(user_id,traveldate,fee,days) VALUES(6,NOW(),7,8)");
        Assert.assertEquals(0, resultSet);
    });
   }
       
       
       
     /**
   	  * DROP EVENT
   	  */
	  public void dropEvent() {
	    using(c -> {
	        int resultSet = c.createStatement().executeUpdate("DROP EVENT IF EXISTS travelrecord_event");
	        Assert.assertEquals(0, resultSet);
	    });
	  }
	  
	  
	/**
	 * ALTER DATABASE
	 */
   public void alterDatabase() {
    using(c -> {
        int resultSet = c.createStatement().executeUpdate("ALTER DATABASE db1 DEFAULT CHARACTER SET = utf8mb4");
        Assert.assertEquals(0, resultSet);
    });
   }
    
}

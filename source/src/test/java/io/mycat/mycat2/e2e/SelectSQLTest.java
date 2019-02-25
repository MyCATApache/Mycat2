package io.mycat.mycat2.e2e;

import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 
 * @author zhanyd
 * @version : V1.0
 * @date : 2018/11/9
 */
public class SelectSQLTest extends BaseSQLTest {
	
	/**
	 * INSERT 
	 */
	    public void insert() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("INSERT INTO travelrecord (user_id,traveldate,fee,days) VALUES('2',now(),21,10)");
            Assert.assertEquals(1, resultSet);
        });
    }

    /**
     * 插入 16 m 的字段
     */
//    @Test
    public void insertSelect16m() {
        // 18874468 > 4194304
        // show VARIABLES like '%max_allowed_packet%'
        // set global max_allowed_packet = 18974468
        // 添加一列 `test16m` longtext,
        using(c -> {
            String sql = "INSERT INTO travelrecord (id,user_id,traveldate,fee,days,test16m) VALUES(999,'999',now(),21,10,?)";
            PreparedStatement statement = c.prepareStatement(sql);
            int size = 1024 * 1024 * 18; // 18 m
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (int i = 0; i < size; i++) {
                out.write('a');
            }
            out.write("END".getBytes());
            statement.setAsciiStream(1,new ByteArrayInputStream(out.toByteArray()));
            int resultSet = statement.executeUpdate();
            Assert.assertEquals(1, resultSet);
        });
    }
	
	
	/**
	 * UPDATE 
	 */
	    public void update() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("UPDATE travelrecord set fee = 11 where id = 1");
            Assert.assertEquals(1, resultSet);
        });
    }
	
	
	/**
	 * INSERT INTO SELECT  
	 */
	    public void insertSelect() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("INSERT INTO travelrecord (user_id,traveldate,fee,days) SELECT user_id,traveldate,fee,days FROM travelrecord WHERE id = 1");
            Assert.assertEquals(1, resultSet);
        });
    }
	
	/**
	 * DELETE 
	 */
	    public void delete() {
        using(c -> {
        	c.createStatement().executeUpdate("DELETE FROM travelrecord WHERE user_id = 2");
        });
    }
	
	/**
	 * SELECT 
	 */
	    public void select() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SELECT * FROM travelrecord");
            Assert.assertTrue(resultSet.next());
        });
    }
	
	
	/**
	 * SELECT INTO
	 */
	    public void selectInto() {
        using(c -> {
            boolean result = c.createStatement().execute("SELECT id,user_id INTO @x,@y FROM travelrecord LIMIT 1");
            Assert.assertFalse(result);
        });
    }
	
	/**
	 * SELECT LOCK IN SHARE MODE
	 */
	    public void selectLockInShareMode() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SELECT * FROM travelrecord LOCK IN SHARE MODE");
            Assert.assertTrue(resultSet.next());
        });
    }
	
	
	/**
	 * SELECT FOR UPDATE
	 */
	    public void selectForUpdate() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SELECT * FROM travelrecord FOR UPDATE");
            Assert.assertTrue(resultSet.next());
        });
    }
	
	
	/**
	 * TRUNCATE 
	 */
	    public void truncate() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("TRUNCATE travelrecord");
        	Assert.assertEquals(0, resultSet);
        });
    }
	
}

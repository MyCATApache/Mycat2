package io.mycat.mycat2.e2e;

import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.Test;

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
	@Test
    public void insert() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("INSERT INTO travelrecord (user_id,traveldate,fee,days) VALUES('2',now(),21,10)");
            Assert.assertEquals(1, resultSet);
        });
    }
	
	
	/**
	 * UPDATE 
	 */
	@Test
    public void update() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("UPDATE travelrecord set fee = 11 where id = 1");
            Assert.assertEquals(1, resultSet);
        });
    }
	
	
	/**
	 * INSERT INTO SELECT  
	 */
	@Test
    public void insertSelect() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("INSERT INTO travelrecord (user_id,traveldate,fee,days) SELECT user_id,traveldate,fee,days FROM travelrecord WHERE id = 1");
            Assert.assertEquals(1, resultSet);
        });
    }
	
	/**
	 * DELETE 
	 */
	@Test
    public void delete() {
        using(c -> {
        	c.createStatement().executeUpdate("DELETE FROM travelrecord WHERE user_id = 2");
        });
    }
	
	/**
	 * SELECT 
	 */
	@Test
    public void select() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SELECT * FROM travelrecord");
            Assert.assertTrue(resultSet.next());
        });
    }
	
	
	/**
	 * SELECT INTO
	 */
	@Test
    public void selectInto() {
        using(c -> {
            boolean result = c.createStatement().execute("SELECT id,user_id INTO @x,@y FROM travelrecord LIMIT 1");
            Assert.assertFalse(result);
        });
    }
	
	/**
	 * SELECT LOCK IN SHARE MODE
	 */
	@Test
    public void selectLockInShareMode() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SELECT * FROM travelrecord LOCK IN SHARE MODE");
            Assert.assertTrue(resultSet.next());
        });
    }
	
	
	/**
	 * SELECT FOR UPDATE
	 */
	@Test
    public void selectForUpdate() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SELECT * FROM travelrecord FOR UPDATE");
            Assert.assertTrue(resultSet.next());
        });
    }
	
	
	/**
	 * TRUNCATE 
	 */
	@Test
    public void truncate() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("TRUNCATE travelrecord");
        	Assert.assertEquals(0, resultSet);
        });
    }
	
}

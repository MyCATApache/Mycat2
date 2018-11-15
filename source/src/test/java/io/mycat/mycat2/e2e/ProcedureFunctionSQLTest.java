package io.mycat.mycat2.e2e;

import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author zhanyd
 * @version : V1.0
 * @date : 2018/11/14
 */
public class ProcedureFunctionSQLTest extends BaseSQLTest{
	
	/**
	 * DROP PROCEDURE 
	 */
	@Test
    public void dropProcedure() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("DROP PROCEDURE IF EXISTS simpleproc");
            Assert.assertEquals(0, resultSet);
        });
    }
	

	/**
	 * CREATE PROCEDURE 
	 */
	@Test
    public void createProcedure() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("CREATE PROCEDURE simpleproc (OUT param1 INT) BEGIN SELECT COUNT(*) INTO @param1 FROM travelrecord; END");
            Assert.assertEquals(0, resultSet);
        });
    }
	
	 /**
     * CALL PROCEDURE
     */
    @Test
    public void callProcedure() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("CALL simpleproc(@a)");
        	Assert.assertEquals(1, resultSet);
        });
    }
    
    
    /**
     * ALTER PROCEDURE
     */
    @Test
    public void alterProcedure() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("ALTER PROCEDURE simpleproc READS SQL DATA COMMENT 'simple'");
        	Assert.assertEquals(0, resultSet);
        });
    }
    
    
    
    /**
	 * DROP FUNCTION 
	 */
	@Test
    public void dropFunction() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("DROP FUNCTION IF EXISTS hello");
            Assert.assertEquals(0, resultSet);
        });
    }
	

	/**
	 * CREATE FUNCTION 
	 */
	@Test
    public void createFunction() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("CREATE FUNCTION hello (s CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ',s,'!')");
            Assert.assertEquals(0, resultSet);
        });
    }
	
	 /**
     * SELECT FUNCTION
     */
    @Test
    public void selectFunction() {
        using(c -> {
        	ResultSet resultSet = c.createStatement().executeQuery("SELECT hello('world')");
        	Assert.assertTrue(resultSet.next());
        });
    }
    
    
    /**
     * ALTER FUNCTION
     */
    @Test
    public void alterFunction() {
        using(c -> {
        	int resultSet = c.createStatement().executeUpdate("ALTER FUNCTION hello READS SQL DATA COMMENT 'hello'");
        	Assert.assertEquals(0, resultSet);
        });
    }
}

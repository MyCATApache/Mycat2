package io.mycat.mycat2.e2e;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author zhanyd
 * @version : V1.0
 * @date : 2018/11/9
 */
public class TableSQLTest extends BaseSQLTest {

	/**
	 * DROP TABLE
	 */
	@Test
    public void dropTable() {
        using(c -> {
            int resultSet = c.createStatement().executeUpdate("DROP TABLE IF EXISTS travelrecord");
            Assert.assertEquals(0, resultSet);
        });
    }
	
	
	/**
	 * CREATE TABLE
	 */
	@Test
    public void createTable() {
        using(c -> {
            int resultSet = c.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS travelrecord (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,user_id varchar(100),traveldate DATE, fee decimal,days int)");
            Assert.assertEquals(0, resultSet);
        });
    }
    
    
    /**
	 * ALTER TABLE
	 */
	@Test
    public void alterTableAddColumn() {
        using(c -> {
            int resultSet = c.createStatement().executeUpdate("ALTER TABLE travelrecord ADD COLUMN `user_name` varchar(20)");
            Assert.assertEquals(0, resultSet);
        });
    }
	
	
	/**
	 * ALTER TABLE
	 */
	@Test
    public void alterTableDropColumn() {
        using(c -> {
            int resultSet = c.createStatement().executeUpdate("ALTER TABLE travelrecord DROP COLUMN `user_name`");
            Assert.assertEquals(0, resultSet);
        });
    }
	
	/**
	 * CREATE INDEX
	 */
	@Test
    public void createIndex() {
        using(c -> {
            int resultSet = c.createStatement().executeUpdate("CREATE INDEX `user_id_index` ON travelrecord (`user_id`) USING BTREE");
            Assert.assertEquals(0, resultSet);
        });
    }
	
	
	
	/**
	 * DROP INDEX
	 */
	@Test
    public void dropIndex() {
        using(c -> {
            int resultSet = c.createStatement().executeUpdate("DROP INDEX `user_id_index` ON travelrecord");
            Assert.assertEquals(0, resultSet);
        });
    }
	
	
}

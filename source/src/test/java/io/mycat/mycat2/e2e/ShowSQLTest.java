package io.mycat.mycat2.e2e;

import org.junit.Assert;

import java.sql.ResultSet;

/**
 * @author : zhuqiang
 * @version : V1.0
 * @date : 2018/11/5 22:54
 */
public class ShowSQLTest extends BaseSQLTest {
    /**
     * SHOW PROCEDURE STATUS
     */
    public void showProcedureStatus() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW PROCEDURE STATUS");
            Assert.assertTrue(resultSet.next());
        });
    }

    public void showProcedureStatus2() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW PROCEDURE STATUS LIKE 'multi'");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW FUNCTION STATUS
     */
    public void showFunctionStatus() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW FUNCTION STATUS");
            Assert.assertTrue(resultSet.next());
        });
    }

    public void showFunctionStatus2() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW FUNCTION STATUS like 'format_bytes'");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW DATABASES
     */
    public void showDatabases() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW DATABASES");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW SCHEMAS
     */
    public void showSchemas() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW SCHEMAS");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW TABLES
     */
    public void showTables() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW TABLES");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW FULL COLUMNS
     */
    public void showFullColumns() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW FULL COLUMNS FROM travelrecord");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW COLUMNS
     */
    public void showColumns() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW COLUMNS FROM travelrecord");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW FIELDS
     */
    public void showFields() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW FIELDS FROM travelrecord FROM db1");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW INDEX
     */
    public void showIndex() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW INDEX FROM travelrecord");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW INDEXES
     */
    public void showIndexes() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW INDEXES FROM travelrecord");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW KEYS
     */
    public void showKeys() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW KEYS FROM travelrecord");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW VARIABLES
     */
    public void showVariables() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW VARIABLES");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW GLOBAL VARIABLES
     */
    public void showGlobalVariables() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW GLOBAL VARIABLES");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW SESSION VARIABLES
     */
    public void showSessionVariables() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW SESSION VARIABLES");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW STATUS
     */
    public void showStatus() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW STATUS");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW GLOBAL STATUS
     */
    public void showGlobalStatus() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW GLOBAL STATUS");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW SESSION STATUS
     */
    public void showSessionStatus() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW SESSION STATUS");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW ENGINE LOGS
     */
    public void showEngineLogs() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW ENGINE INNODB LOGS");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW ENGINE STATUS
     */
    public void showEngineStatus() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW ENGINE INNODB STATUS");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW ENGINE MUTEX
     */
    public void showEngineMutex() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW ENGINE INNODB MUTEX");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW PROCESSLIST
     */
    public void showProcesslist() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW PROCESSLIST");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW FULL PROCESSLIST
     */
    public void showFullProcesslist() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW FULL PROCESSLIST");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW MASTER STATUS
     */
    public void showMasterStatus() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW MASTER STATUS");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW SLAVE STATUS
     */
    public void showSlaveStatus() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW SLAVE STATUS");
            Assert.assertTrue(resultSet.next());
        });
    }

    /**
     * SHOW GRANTS
     */
    public void showGrants() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW GRANTS");
            Assert.assertTrue(resultSet.next());
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
     * SHOW CREATE FUNCTION
     */
    public void showCreateFunction() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW CREATE FUNCTION hello");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW CREATE PROCEDURE
     */
    public void showCreateProcedure() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW CREATE PROCEDURE simpleproc");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW CREATE TABLE
     */
    public void showCreateTable() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW CREATE TABLE travelrecord");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW CREATE TRIGGER
     */
    public void showCreateTrigger() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW CREATE TRIGGER trigger_name");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW CREATE VIEW
     */
    public void showCreateView() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW CREATE VIEW view_name");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW CHARACTER SET
     */
    public void showCharacterSet() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW CHARACTER SET LIKE 'latin%'");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW COLLATION
     */
    public void showCollation() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW COLLATION WHERE Charset = 'latin1'");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW CREATE DATABASE
     */
    public void showCreateDatabase() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW CREATE DATABASE db1");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * SHOW CREATE SCHEMA
     */
    public void showCreateSchema() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("SHOW CREATE SCHEMA db1");
            Assert.assertTrue(resultSet.next());
        });
    }


    /**
     * CHECKSUM TABLE
     */
    public void checksumTable() {
        using(c -> {
            ResultSet resultSet = c.createStatement().executeQuery("CHECKSUM TABLE travelrecord");
            Assert.assertTrue(resultSet.next());
        });
    }
}

package io.mycat.mycat2.e2e;

import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;

/**
 * @author : zhuqiang
 * @version : V1.0
 * @date : 2018/11/5 23:29
 */
public class PrepareSQLTest extends BaseSQLTest {
    /** SQLCOM_PREPARE、  SQLCOM_EXECUTE、  SQLCOM_DEALLOCATE_PREPARE  */
    @Test
    public void sqlcomPrepare() {
        // TODO: 2018/11/7 jdbc 不知道怎么写
        using(c -> {
//            CallableStatement callableStatement = c.prepareCall("SET @skip=1; SET @numrows=5; PREPARE STMT FROM 'SELECT * FROM travelrecord LIMIT ?, ?';\n" +
//                    "EXECUTE STMT USING @skip, @numrows;");
            ResultSet resultSet = c.createStatement().executeQuery("SET @skip=1; SET @numrows=5; PREPARE STMT FROM 'SELECT * FROM travelrecord LIMIT ?, ?';\n" +
                    "EXECUTE STMT USING @skip, @numrows;");
            Assert.assertTrue(resultSet.next());
//            boolean execute = callableStatement.execute();
//            System.out.println(callableStatement);
        });
    }
}

package io.mycat.sysbench;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import lombok.SneakyThrows;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class SysbenchTest implements MycatTest {

    @Test
    @SneakyThrows
    public void baseTest(){

        try(Connection mySQLConnection = getMySQLConnection(DB_MYCAT_PSTMT);){
            JdbcUtils.execute(mySQLConnection,"create database testdb");
            JdbcUtils.execute(mySQLConnection,"use testdb");
            JdbcUtils.execute(mySQLConnection,"CREATE TABLE IF NOT EXISTS testdb.sbtest1 (\n" +
                    "\tid INTEGER NOT NULL AUTO_INCREMENT,\n" +
                    "\tk INTEGER ,\n" +
                    "\tc CHAR(120) ,\n" +
                    "\tpad CHAR(60),\n" +
                    "\tPRIMARY KEY (id),\n" +
                    "\tINDEX k_1(k)\n" +
                    ")");
            JdbcUtils.execute(mySQLConnection,"insert sbtest1(id,k,c,pad) " +
                    "values (1,25," +
                    "'31451373586-15688153734-79729593694-96509299839-83724898275-86711833539-78981337422-35049690573-51724173961-87474696253'," +
                    "'98996621624-36689827414-04092488557-09587706818-65008859162')");
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mySQLConnection, "SELECT c FROM testdb.sbtest1 WHERE id=?;", Arrays.asList(1));
            System.out.println();
        }
    }
}

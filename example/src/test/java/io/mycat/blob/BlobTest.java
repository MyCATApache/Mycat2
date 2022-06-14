package io.mycat.blob;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class BlobTest implements MycatTest {
    @Test
    public void testBase() throws Exception {
        case1(DB_MYCAT);
    }

    @Test
    public void testBase2() throws Exception {
        case1(DB_MYCAT_PSTMT);
    }

    @Test(expected = java.io.StreamCorruptedException.class)
    public void testBase3() throws Exception {
        case2(DB_MYCAT);
    }

    @Test()
    public void testBase4() throws Exception {
        case2(DB_MYCAT_PSTMT);
    }

    private void case2(String url) throws Exception {
        try (Connection jdbcConnection = getMySQLConnection(url)) {
            execute(jdbcConnection, RESET_CONFIG);
            execute(jdbcConnection,"create database db1");
            execute(jdbcConnection, "CREATE TABLE db1.`testBlob` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
            deleteData(jdbcConnection,"db1","testBlob");

            ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
            int size = 6 * 1024 * 1024;
            int[] originalIns = threadLocalRandom.ints().limit(size).toArray();
            ByteArrayOutputStream byteArrayOutputStream =
                    new ByteArrayOutputStream(size);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(originalIns);
            byte[] originalBytes = byteArrayOutputStream.toByteArray();
            byteArrayOutputStream.close();
            objectOutputStream.close();

            Blob blob = jdbcConnection.createBlob();
            blob.setBytes(1,originalBytes);
            JdbcUtils.execute(jdbcConnection,"insert db1.testBlob (`blob`) values(?)",Arrays.asList(blob));
            List<Map<String, Object>> maps = executeQuery(jdbcConnection, "select * from db1.testBlob");
            byte[] receiveBlob =(byte[]) maps.get(0).get("blob");

            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(receiveBlob));
            int[] receiveInts= (int[])in.readObject();
            in.close();

            Assert.assertArrayEquals(originalIns,receiveInts);

            System.out.println();
            System.out.println();

        }
    }

    private void case1(String url) throws Exception {
        try (Connection mycatConnection = getMySQLConnection(url)) {
            execute(mycatConnection, RESET_CONFIG);
            execute(mycatConnection,"create database db1");
            execute(mycatConnection, "CREATE TABLE db1.`testBlob` (\n" +
                    "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                    "  `blob` longblob,\n" +
                    "  PRIMARY KEY (`id`),\n" +
                    "  KEY `id` (`id`)\n" +
                    ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
            deleteData(mycatConnection,"db1","testBlob");

            ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
           int size = 6 * 1024 * 1024;
            int[] originalIns = threadLocalRandom.ints().limit(size).toArray();
            ByteArrayOutputStream byteArrayOutputStream =
                    new ByteArrayOutputStream(size);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(originalIns);

            byteArrayOutputStream.close();
            objectOutputStream.close();
            byte[] originalBytes = byteArrayOutputStream.toByteArray();
            JdbcUtils.execute(mycatConnection,"insert db1.testBlob (`blob`) values(?)",Arrays.asList(originalBytes));
            List<Map<String, Object>> maps = executeQuery(mycatConnection, "select * from db1.testBlob");
            byte[] receiveBlob =(byte[]) maps.get(0).get("blob");

            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(receiveBlob));
            int[] receiveInts= (int[])in.readObject();
            in.close();

            Assert.assertArrayEquals(originalIns,receiveInts);

            System.out.println();
            System.out.println();

        }
    }


}

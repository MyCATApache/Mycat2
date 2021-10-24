package io.mycat.config;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.sqlhandler.config.DbStorageManagerImpl;
import io.mycat.sqlhandler.config.FileStorageManagerImpl;
import io.mycat.sqlhandler.config.KV;
import io.mycat.sqlhandler.config.StdStorageManagerImpl;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;
import org.sparkproject.guava.io.Files;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.*;

public class MycatServerStdStorageManagerTest implements MycatTest {
    public static final String syncConfigFromDbToFile = "/*+mycat:syncConfigFromDbToFile{} */";
    public static final String syncConfigFromFileToDb = "/*+mycat:syncConfigFromFileToDb{} */";
    public static final String checkConfigConsistency = "/*+mycat:checkConfigConsistency{} */";
    public static final String loadConfigFromFile = "/*+mycat:loadConfigFromFile{} */";

    @Test
    @SneakyThrows
    public void test() {
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);) {
            JdbcUtils.execute(mySQLConnection, RESET_CONFIG);
            JdbcUtils.execute(mySQLConnection, syncConfigFromFileToDb);



            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mySQLConnection, checkConfigConsistency, Collections.emptyList());
            Assert.assertEquals("[{value=1}]", maps.toString());

            DatasourceConfig ds = CreateDataSourceHint.createConfig("prototypeDs", DB1);
            DbStorageManagerImpl dbStorageManager = new DbStorageManagerImpl(ds);
            dbStorageManager.register(DatasourceConfig.class);
            KV<DatasourceConfig> datasourceConfigKV = dbStorageManager.get(DatasourceConfig.class);
            String tmpName = System.currentTimeMillis() + "";
            datasourceConfigKV.put(tmpName, CreateDataSourceHint.createConfig(tmpName, DB1));

            maps = JdbcUtils.executeQuery(mySQLConnection, checkConfigConsistency, Collections.emptyList());
            Assert.assertEquals("[{value=0}]", maps.toString());
            JdbcUtils.execute(mySQLConnection, syncConfigFromFileToDb);
            maps = JdbcUtils.executeQuery(mySQLConnection, checkConfigConsistency, Collections.emptyList());
            Assert.assertEquals("[{value=1}]", maps.toString());

            Thread.sleep(1);
            tmpName = System.currentTimeMillis() + "";
            datasourceConfigKV.put(tmpName, CreateDataSourceHint.createConfig(tmpName, DB1));
            JdbcUtils.execute(mySQLConnection, syncConfigFromDbToFile);
            JdbcUtils.execute(mySQLConnection, loadConfigFromFile);
            maps = executeQuery(mySQLConnection, "/*+mycat:showDatasources{}*/");

            Assert.assertTrue(maps.toString().contains(tmpName));
        }

    }

    @Test
    @SneakyThrows
    public void test2() {
        DatasourceConfig datasourceConfig = CreateDataSourceHint.createConfig("prototype", DB1);
        DbStorageManagerImpl dbStorageManager = new DbStorageManagerImpl(datasourceConfig);

        try (Connection mySQLConnection = getMySQLConnection(DB1);) {
            deleteData(mySQLConnection, "mycat", "replica_log");
        }

        dbStorageManager.reportReplica(Collections.singletonMap("1", Arrays.asList("2", "3")));
        try (Connection mySQLConnection = getMySQLConnection(DB1);) {
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mySQLConnection, "select * from mycat.replica_log", Collections.emptyList());
            Assert.assertTrue(!maps.isEmpty());
        }

        dbStorageManager.register(DatasourceConfig.class);
        Assert.assertEquals(1, dbStorageManager.registerClasses().size());
        KV<DatasourceConfig> datasourceConfigKV = dbStorageManager.get(DatasourceConfig.class);
        KVTest.test(datasourceConfigKV);
    }

    @Test
    @SneakyThrows
    public void test3() {
        File tempDir = Files.createTempDir();
        Path path = tempDir.toPath();
        FileStorageManagerImpl fileStorageManager = new FileStorageManagerImpl(path);
        StdStorageManagerImpl stdStorageManager = new StdStorageManagerImpl(fileStorageManager);

        stdStorageManager.register(DatasourceConfig.class);
        Collection<Class> classes = stdStorageManager.registerClasses();
        Assert.assertEquals(1, classes.size());
        KV<DatasourceConfig> datasourceConfigKV = stdStorageManager.get(DatasourceConfig.class);
        KVTest.test(datasourceConfigKV);
    }

    @Test
    @SneakyThrows
    public void test4() {
        File tempDir = Files.createTempDir();
        Path path = tempDir.toPath();
        FileStorageManagerImpl fileStorageManager = new FileStorageManagerImpl(path);
        StdStorageManagerImpl stdStorageManager = new StdStorageManagerImpl(fileStorageManager);

        stdStorageManager.register(DatasourceConfig.class);
        Collection<Class> classes = stdStorageManager.registerClasses();
        Assert.assertEquals(1, classes.size());

        DatasourceConfig datasourceConfig = CreateDataSourceHint.createConfig("prototype", DB1);
        KV<DatasourceConfig> datasourceConfigKV = fileStorageManager.get(DatasourceConfig.class);
        datasourceConfigKV.put(datasourceConfig.getName(), datasourceConfig);

        Optional<DbStorageManagerImpl> dbStorageManagerOptional = stdStorageManager.getDbStorageManager();
        Assert.assertTrue(dbStorageManagerOptional.isPresent());

        System.out.println();
    }

    @Test
    @SneakyThrows
    public void test5() {
        File tempDir = Files.createTempDir();
        Path path = tempDir.toPath();
        FileStorageManagerImpl fileStorageManager = new FileStorageManagerImpl(path);
        StdStorageManagerImpl stdStorageManager = new StdStorageManagerImpl(fileStorageManager);

        stdStorageManager.register(DatasourceConfig.class);
        Collection<Class> classes = stdStorageManager.registerClasses();
        Assert.assertEquals(1, classes.size());

        DatasourceConfig datasourceConfig = CreateDataSourceHint.createConfig("prototypeDs", DB1);
        KV<DatasourceConfig> datasourceConfigKV = fileStorageManager.get(DatasourceConfig.class);
        datasourceConfigKV.put(datasourceConfig.getName(), datasourceConfig);

        Optional<DbStorageManagerImpl> dbStorageManagerOptional = stdStorageManager.getDbStorageManager();
        Assert.assertTrue(dbStorageManagerOptional.isPresent());

        System.out.println();
    }

    @Test
    @SneakyThrows
    public void test6() {
        File tempDir = Files.createTempDir();
        Path path = tempDir.toPath();
        FileStorageManagerImpl fileStorageManager = new FileStorageManagerImpl(path);
        StdStorageManagerImpl stdStorageManager = new StdStorageManagerImpl(fileStorageManager);

        stdStorageManager.register(DatasourceConfig.class);
        Collection<Class> classes = stdStorageManager.registerClasses();
        Assert.assertEquals(1, classes.size());


        DatasourceConfig datasourceConfig = CreateDataSourceHint.createConfig("prototypeDs", DB1);
        KV<DatasourceConfig> datasourceConfigKV = fileStorageManager.get(DatasourceConfig.class);
        datasourceConfigKV.put(datasourceConfig.getName(), datasourceConfig);

        Optional<DbStorageManagerImpl> dbStorageManagerOptional = stdStorageManager.getDbStorageManager();

        DbStorageManagerImpl dbStorageManager = dbStorageManagerOptional.get();
        KV<DatasourceConfig> dbKV = dbStorageManager.get(DatasourceConfig.class);
        dbKV.clear();

        Assert.assertEquals(0, dbKV.values().size());
        stdStorageManager.syncToNet();
        Assert.assertEquals(1, dbKV.values().size());


        DatasourceConfig testDatasourceConfig = CreateDataSourceHint.createConfig("ds1", DB1);
        datasourceConfigKV.put(testDatasourceConfig.getName(), testDatasourceConfig);
        Assert.assertEquals(2, datasourceConfigKV.values().size());

        Assert.assertEquals(1, dbKV.values().size());
        stdStorageManager.syncToNet();
        Assert.assertEquals(2, dbKV.values().size());

        datasourceConfigKV.removeKey(testDatasourceConfig.getName());
        Assert.assertEquals(1, datasourceConfigKV.values().size());
        Assert.assertEquals(2, dbKV.values().size());

        stdStorageManager.syncFromNet();
        Assert.assertEquals(dbKV.values().size(), datasourceConfigKV.values().size());
        System.out.println();
    }

    public static void main(String[] args) {


        System.out.println();
    }
}

package io.mycat.config;

import io.mycat.assemble.MycatTest;
import io.mycat.hint.CreateDataSourceHint;
import io.mycat.sqlhandler.config.DbKVImpl;
import io.mycat.sqlhandler.config.FileKV;
import io.mycat.sqlhandler.config.KV;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class KVTest implements MycatTest {


    @SneakyThrows
    @Test
    public void testFileKV() {

        Class aClass = DatasourceConfig.class;
        KV fileKV = getFileKV("datasources",aClass);
        test(fileKV);
    }

    @SneakyThrows
    @Test
    public void testMySQLKV() {
        Class aClass = DatasourceConfig.class;
        KV fileKV = getDbKV("datasources",aClass);
        test(fileKV);
    }

    public static void test(KV kv) {
        List<KVObject> values1 = kv.values();
        for (KVObject value : values1) {
            kv.removeKey(value.keyName());
        }

        String key = "key1";
        DatasourceConfig datasourceConfig = CreateDataSourceHint.createConfig("key",DB1);
        datasourceConfig.setName(key);
        datasourceConfig.setUrl(DB1);
        datasourceConfig.setUser("root");
        kv.put(datasourceConfig.getName(), datasourceConfig);

        Assert.assertEquals(datasourceConfig, kv.get(key).get());

        datasourceConfig.setDbType("o");
        Assert.assertNotEquals(datasourceConfig, kv.get(key));

        kv.put(datasourceConfig.getName(), datasourceConfig);
        Assert.assertEquals(datasourceConfig, kv.get(key).get());

        List values = kv.values();
        Assert.assertEquals(1, values.size());

        Assert.assertEquals(datasourceConfig, values.get(0));

        kv.removeKey(datasourceConfig.getName());

        values = kv.values();
        Assert.assertEquals(0, values.size());

        System.out.println();
    }

    @NotNull
    private KV getFileKV(String path,Class aClass) throws IOException {
        Path tempDirectory = Files.createTempDirectory("mycatTest");
        return new FileKV<>("testtemplate", aClass, tempDirectory.toAbsolutePath(), "s_");
    }

    @NotNull
    private KV getDbKV(String path,Class aClass) throws IOException {
        DatasourceConfig ds = CreateDataSourceHint.createConfig("ds", DB1);
        return new DbKVImpl<>(ds,path, aClass);
    }

}
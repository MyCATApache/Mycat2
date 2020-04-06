package io.mycat.example.sharding;

import io.mycat.ConfigProvider;
import io.mycat.MycatCore;
import io.mycat.RootHelper;
import lombok.SneakyThrows;

import java.nio.file.Paths;

public class ShardingExample {
    @SneakyThrows
    public static void main(String[] args) {
        String resource = Paths.get( ShardingExample.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(ShardingExample.class);
        MycatCore.INSTANCE.init(bootConfig);
    }
}
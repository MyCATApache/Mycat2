package io.mycat.example.readWriteSeparation;

import io.mycat.ConfigProvider;
import io.mycat.MycatCore;
import io.mycat.RootHelper;
import lombok.SneakyThrows;

import java.nio.file.Paths;

public class ReadWriteSeparationExample {
    @SneakyThrows
    public static void main(String[] args) throws Exception {
        String resource = Paths.get( ReadWriteSeparationExample.class.getResource("").toURI()).toAbsolutePath().toString();
        System.out.println(resource);
        System.setProperty("MYCAT_HOME", resource);
        ConfigProvider bootConfig = RootHelper.INSTANCE.bootConfig(ReadWriteSeparationExample.class);
        MycatCore.INSTANCE.init(bootConfig);
    }
}